import asyncio
import logging
import os
import shutil
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List
from uuid import uuid4

import nodriver as uc

import utils
import metrics as m

logger = logging.getLogger(__name__)

LOOP_CALL_TIMEOUT = 120.0   # bound for coroutine_threadsafe awaits (tab boot/nav/close)


class MaxTabsReachedError(Exception):
    """Raised when max primary tabs limit is reached."""
    pass


async def _launch_browser():
    """Launch the shared nodriver Chrome (headed under Xvfb when HEADLESS=false).
    MUST be called on the ChromeManager event loop (async)."""
    headless = utils.get_config_headless()
    if not headless:
        utils.start_xvfb_display()          # idempotent; shares existing Xvfb
    user_data_dir = tempfile.mkdtemp(prefix="tabmgr_")
    sandbox = os.environ.get('WS_CHROME_SANDBOX', 'false').lower() == 'true'
    try:
        browser = await uc.start(
            headless=headless,
            sandbox=sandbox,
            user_data_dir=user_data_dir,
            browser_args=[
                "--no-first-run",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1920,1080",
                "--site-per-process",
                "--enable-features=IsolateOrigins,site-per-process",
                f"--js-flags=--max-old-space-size={utils.get_config_ws_chrome_v8_heap_mb()}",
            ],
        )
        _launch_browser.user_data_dir = user_data_dir
        return browser
    except Exception:
        shutil.rmtree(user_data_dir, ignore_errors=True)
        raise


async def _launch_standby_browser():
    """Launch an isolated standby nodriver Chrome with optimized footprint.
    MUST be called on the ChromeManager event loop (async)."""
    headless = utils.get_config_headless()
    if not headless:
        utils.start_xvfb_display()
    user_data_dir = tempfile.mkdtemp(prefix="tabmgr_standby_")
    sandbox = os.environ.get('WS_CHROME_SANDBOX', 'false').lower() == 'true'
    try:
        browser = await uc.start(
            headless=headless,
            sandbox=sandbox,
            user_data_dir=user_data_dir,
            port=0,
            browser_args=[
                "--no-first-run",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--blink-settings=imagesEnabled=false",
                "--window-size=800,600",
                "--js-flags=--max-old-space-size=256",
                "--site-per-process",
                "--enable-features=IsolateOrigins,site-per-process",
            ],
        )
        browser.user_data_dir = user_data_dir
        try:
            import flaresolverr_service as fs
            fs.register_shared_browser_dir(user_data_dir)
        except Exception as e:
            logger.warning(f"Failed to register standby browser dir: {e}")
        return browser
    except Exception:
        shutil.rmtree(user_data_dir, ignore_errors=True)
        raise


async def launch_standby_browser():
    """Top-level coroutine to launch a standby browser."""
    return await _launch_standby_browser()


def _is_data_frame_fast(payload: str) -> bool:
    """Fast-path classification of data vs control frames without JSON parse/hash overhead."""
    if not payload:
        return False
    stripped = payload.strip()
    if stripped in ("2", "3", "ping", "pong", "PING", "PONG"):
        return False
    # Check control / heartbeat markers
    if '"heartbeat"' in stripped or '"ping"' in stripped or '"pong"' in stripped:
        return False
    if '"major_coin_price"' in stripped or '"chain_stat"' in stripped:
        return False
    return True


class RollingRateCounter:
    """Zero-allocation 60-second rolling rate counter."""
    __slots__ = ('_buckets', '_current_sec', '_lock')

    def __init__(self, num_buckets: int = 60):
        self._buckets = [0] * num_buckets
        self._current_sec = int(time.time())
        self._lock = threading.Lock()

    def inc(self, now: float):
        sec = int(now)
        with self._lock:
            if sec != self._current_sec:
                diff = min(sec - self._current_sec, len(self._buckets))
                for i in range(diff):
                    idx = (self._current_sec + 1 + i) % len(self._buckets)
                    self._buckets[idx] = 0
                self._current_sec = sec
            self._buckets[sec % len(self._buckets)] += 1

    def rate(self, window_s: float = 60.0) -> float:
        if window_s <= 0:
            return 0.0
        now = time.time()
        sec = int(now)
        with self._lock:
            if sec != self._current_sec:
                diff = min(sec - self._current_sec, len(self._buckets))
                for i in range(diff):
                    idx = (self._current_sec + 1 + i) % len(self._buckets)
                    self._buckets[idx] = 0
                self._current_sec = sec
            total = sum(self._buckets)
        return total / window_s


@dataclass
class TabState:
    tab_id: str
    url: str
    tab: object                            # nodriver Tab (asyncio-bound)
    target_id: str
    frame_buffer: deque = field(default_factory=lambda: deque(maxlen=utils.get_config_ws_listener_default_max_msgs()))
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_frame_ts: float = field(default_factory=time.time)
    status: str = "starting"             # starting | running | warming | handoff | retiring | crashed | reloading
    service_started_at: datetime = field(default_factory=datetime.now)
    handlers: list = field(default_factory=list)   # registered CDP handler refs
    last_data_frame_ts: float = 0.0
    last_control_frame_ts: float = 0.0
    consecutive_stalls: int = 0
    data_frame_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    control_frame_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    _data_rate_counter: RollingRateCounter = field(default_factory=RollingRateCounter)
    _control_rate_counter: RollingRateCounter = field(default_factory=RollingRateCounter)

    def data_frame_rate(self, window_s: float = 60.0) -> float:
        """Calculate rolling data frames per second over the specified window."""
        if window_s <= 0:
            return 0.0
        now = time.time()
        cutoff = now - window_s
        with self.lock:
            if not self.data_frame_history:
                return 0.0
            count = sum(1 for ts in self.data_frame_history if ts >= cutoff)
        return count / window_s

    def control_frame_rate(self, window_s: float = 60.0) -> float:
        """Calculate rolling control/ping frames per second over the specified window."""
        if window_s <= 0:
            return 0.0
        now = time.time()
        cutoff = now - window_s
        with self.lock:
            if not self.control_frame_history:
                return 0.0
            count = sum(1 for ts in self.control_frame_history if ts >= cutoff)
        return count / window_s

    def _handle_frame(self, frame_type: str, payload: str, cdp_ts=None):
        if not isinstance(payload, str):
            payload = str(payload or "")
        MAX_FRAME_SIZE = 1_000_000
        if len(payload) > MAX_FRAME_SIZE:
            logger.warning(f"Dropping oversized WS frame ({len(payload)} bytes) for {self.url}")
            return

        is_data = _is_data_frame_fast(payload)

        now = time.time()
        msg = {
            "timestamp": now,
            "type": frame_type,                      # "webSocketFrameReceived" | "webSocketFrameSent"
            "url": self.url,
            "payload": payload,
            "cdp_ts": cdp_ts,                        # CDP MonotonicTime; INTERNAL
        }
        with self.lock:
            self.frame_buffer.append(msg)
            self.last_frame_ts = now
            if is_data:
                self.last_data_frame_ts = now
                self.data_frame_history.append(now)
                self.consecutive_stalls = 0
            else:
                self.last_control_frame_ts = now
                self.control_frame_history.append(now)

        if is_data:
            self._data_rate_counter.inc(now)
        else:
            self._control_rate_counter.inc(now)

    def _feed(self, frame_type: str, payload: str, cdp_ts=None):
        self._handle_frame(frame_type, payload, cdp_ts=cdp_ts)


class ChromeManager:
    """Single shared nodriver Chrome with per-URL tabs.

    Runs nodriver on a dedicated asyncio loop in a daemon thread. All public
    methods are thread-safe; nodriver calls are marshalled to the loop."""

    def __init__(self, max_tabs: int = 5):
        self.max_tabs = max_tabs
        self._browser = None
        self._tabs: Dict[str, TabState] = {}      # tab_id -> TabState
        self._url_index: Dict[str, str] = {}       # url -> primary tab_id (mirrors WebSocketListenerManager)
        self._lock = threading.RLock()          # registry lock (sync side)
        self._running = False
        self._restart_lock = threading.Lock()   # single-flight restart
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        # Bounded recycle executor (B1): all tab ops serialize on the single
        # asyncio loop, so max_workers=1 (any more is oversubscription) and it
        # bounds the max_tabs*2 shadow-cap overshoot. Created here so it is lazy
        # (never launches Chrome by itself in tests); re-created in start() if a
        # previous stop() shut it down, and only shut down on stop().
        self._recycle_executor: Optional[ThreadPoolExecutor] = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="tab_recycle")

    # ---- async internals (run on self._loop) ---------------------------

    async def _async_start(self):
        self._browser = await _launch_browser()
        # Capture shared browser's user_data_dir for orphan-sweeper registration
        self._shared_browser_user_data_dir = getattr(_launch_browser, "user_data_dir", None)

    async def _async_create_tab_registered(self, url: str, tab_id: str, is_shadow: bool) -> TabState:
        """Browser-launch + CDP-attach + registry registration as ONE async unit.
        Runs on the loop; used both by create_tab() and by _recover() (loop-crash
        watchdog). Using this single async unit means recovery never deadlocks on
        _call()/run_coroutine_threadsafe (which would block while the loop is
        pumped via run_until_complete outside run_forever)."""
        tab = await self._browser.get(url)
        tab_state = TabState(
            tab_id=tab_id,
            url=url,
            tab=tab,
            target_id=getattr(tab, "target_id", ""),
        )
        await self._async_attach_cdp(tab_state)
        with self._lock:
            self._tabs[tab_id] = tab_state
            if not is_shadow:
                self._url_index[url] = tab_id
        return tab_state

    async def _async_attach_cdp(self, tab_state: TabState):
        tab = tab_state.tab
        try:
            await asyncio.wait_for(tab.send(uc.cdp.network.enable()), timeout=5.0)
            if hasattr(uc.cdp, "inspector") and hasattr(uc.cdp.inspector, "enable"):
                try:
                    await asyncio.wait_for(tab.send(uc.cdp.inspector.enable()), timeout=5.0)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"CDP domain enable error: {e}")

        async def on_received(event: uc.cdp.network.WebSocketFrameReceived):
            # event.timestamp is the CDP MonotonicTime, shared by ALL tabs in
            # this Chrome — the strongest cross-tab "same message" signal.
            try:
                cdp_ts = getattr(event, "timestamp", None)
                tab_state._feed("webSocketFrameReceived",
                                getattr(event.response, "payload_data", ""),
                                cdp_ts=cdp_ts)
            except Exception as e:
                logger.error(f"CDP on_received handler error: {e}")
                return

        async def on_sent(event: uc.cdp.network.WebSocketFrameSent):
            try:
                cdp_ts = getattr(event, "timestamp", None)
                tab_state._feed("webSocketFrameSent",
                                getattr(event.response, "payload_data", ""),
                                cdp_ts=cdp_ts)
            except Exception as e:
                logger.error(f"CDP on_sent handler error: {e}")
                return

        async def on_detached(event: uc.cdp.inspector.Detached):
            logger.warning(f"CDP Inspector detached for tab {tab_state.tab_id} ({tab_state.url}): {event}")
            tab_state.status = "crashed"

        async def on_target_crashed(event: uc.cdp.target.TargetCrashed):
            logger.warning(f"CDP Target crashed for tab {tab_state.tab_id} ({tab_state.url}): {event}")
            tab_state.status = "crashed"

        tab.add_handler(uc.cdp.network.WebSocketFrameReceived, on_received)
        tab.add_handler(uc.cdp.network.WebSocketFrameSent, on_sent)
        handlers = [on_received, on_sent]

        if hasattr(uc.cdp, "inspector") and hasattr(uc.cdp.inspector, "Detached"):
            tab.add_handler(uc.cdp.inspector.Detached, on_detached)
            handlers.append(on_detached)

        if hasattr(uc.cdp, "target") and hasattr(uc.cdp.target, "TargetCrashed"):
            tab.add_handler(uc.cdp.target.TargetCrashed, on_target_crashed)
            handlers.append(on_target_crashed)

        if hasattr(uc.cdp, "inspector") and hasattr(uc.cdp.inspector, "TargetCrashed"):
            tab.add_handler(uc.cdp.inspector.TargetCrashed, on_target_crashed)

        tab_state.handlers = handlers

    async def _async_soft_reload_tab(self, tab_id: str) -> bool:
        with self._lock:
            tab_state = self._tabs.get(tab_id)
        if not tab_state:
            logger.warning(f"ChromeManager: soft reload failed - tab {tab_id} not found")
            return False

        tab_state.status = "reloading"
        try:
            tab = tab_state.tab
            await asyncio.wait_for(tab.get(tab_state.url), timeout=10.0)
            try:
                await asyncio.wait_for(tab.send(uc.cdp.network.enable()), timeout=5.0)
                if hasattr(uc.cdp, "inspector") and hasattr(uc.cdp.inspector, "enable"):
                    await asyncio.wait_for(tab.send(uc.cdp.inspector.enable()), timeout=5.0)
            except Exception:
                pass
            tab_state.status = "running"
            tab_state.consecutive_stalls = 0
            logger.info(f"ChromeManager: soft reload succeeded for tab {tab_id} ({tab_state.url})")
            return True
        except Exception as e:
            logger.warning(f"ChromeManager: soft reload failed for tab {tab_id} ({tab_state.url}): {e}")
            tab_state.status = "crashed"
            return False

    async def _async_close_tab(self, tab_state: TabState):
        try:
            await asyncio.wait_for(tab_state.tab.close(), timeout=5.0)
        except Exception:
            logger.debug(f"close tab {tab_state.url} (already gone or timed out)")

    async def _async_stop(self):
        if self._browser is not None:
            try:
                # nodriver Browser.stop() is a SYNC method (terminates the OS
                # process); it must not be awaited (awaiting its None return would
                # raise TypeError). We run it directly on the loop and guard it.
                stop = getattr(self._browser, "stop", None)
                if callable(stop):
                    stop()
            except Exception as e:
                logger.warning(f"browser stop error: {e}")
            self._browser = None

    # ---- loop marshalling helpers (call from any thread) ----------------

    def _call(self, coro, timeout: float = LOOP_CALL_TIMEOUT):
        if self._loop is None or self._loop.is_closed() or not self._loop.is_running():
            raise RuntimeError("ChromeManager event loop not running")
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=timeout)
        except Exception:
            fut.cancel()
            raise

    # ---- legacy metric accounting (single source of truth = registry) ---

    def _live_primary_count(self) -> int:
        """Number of urls with a live primary tab (== WS_LISTENERS_ACTIVE)."""
        with self._lock:
            return sum(1 for tid in self._url_index.values() if tid in self._tabs)

    def _sync_url_metrics(self, url: str, reset_uptime: bool = False) -> None:
        """Align the per-URL legacy gauges with the registry.

        WS_LISTENER_ACTIVE is 1 when `url` has a live primary tab, else 0.
        When reset_uptime is set (the url JUST became a live primary — first
        create or post-recover recreate) WS_LISTENER_UPTIME is (re)marked from the
        tab's service_started_at anchor as ELAPSED SECONDS (matching the legacy
        semantics in metrics.py — NOT a wall-clock timestamp). Shadow warming and
        handoff retires must NOT reset it so uptime stays continuous."""
        with self._lock:
            tid = self._url_index.get(url)
        live = tid is not None and tid in self._tabs
        m.WS_LISTENER_ACTIVE.labels(url=url).set(1 if live else 0)
        if live and reset_uptime:
            with self._lock:
                tab = self._tabs.get(tid)
            if tab is not None:
                m.WS_LISTENER_UPTIME.labels(url=url).set(
                    (datetime.now() - tab.service_started_at).total_seconds())

    def _recompute_ws_gauges(self, reset_uptime: bool = False) -> None:
        """Recompute all legacy gauges from the registry (never blind inc/dec).

        WS_LISTENERS_ACTIVE is set to the exact number of live primaries and every
        per-URL gauge is re-synced from the registry."""
        with self._lock:
            urls = list(self._url_index.keys())
        m.WS_LISTENERS_ACTIVE.set(self._live_primary_count())
        for url in urls:
            self._sync_url_metrics(url, reset_uptime=reset_uptime)

    # ---- public thread-safe API -----------------------------------------

    def start(self):
        if self._running:
            return
        # Re-create the recycle executor if a previous stop() shut it down (e.g.
        # the internal stop() inside restart_browser()); keeps it idempotent
        # across start()/stop()/restart_browser() cycles.
        if self._recycle_executor is None:
            self._recycle_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="tab_recycle")
        logger.info("ChromeManager: starting shared nodriver Chrome (dedicated loop)")
        self._loop = asyncio.new_event_loop()

        def _run():
            asyncio.set_event_loop(self._loop)
            backoff = 1.0
            max_backoff = 60.0
            while self._running:
                try:
                    self._loop.run_forever()
                except Exception as e:
                    logger.error(f"ChromeManager event loop crashed: {e}, recovering in {backoff:.1f}s")
                    # Mark the loop dead BEFORE the backoff sleep so any concurrent
                    # _call() fast-fails (RuntimeError) instead of queuing a coroutine
                    # onto the crashed-but-not-closed loop and stalling up to
                    # LOOP_CALL_TIMEOUT. _recover() installs a fresh loop.
                    self._loop = None
                    time.sleep(backoff)
                    if self._recover():
                        backoff = 1.0
                    else:
                        backoff = min(backoff * 2, max_backoff)
                else:
                    break

        self._loop_thread = threading.Thread(target=_run, daemon=True, name="chrome_manager_loop")
        # Set _running BEFORE starting the loop thread / launching Chrome: if the
        # launch fails, the daemon loop would otherwise spin in run_forever() forever
        # and leave the manager unrecoverable. On failure we wind the loop down and
        # re-raise so the manager stays clean and recoverable.
        self._running = True
        self._loop_thread.start()
        try:
            self._call(self._async_start())
        except Exception:
            logger.error("ChromeManager: Chrome launch failed; stopping loop thread")
            self._running = False
            if self._loop is not None and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self._loop.stop)
            if self._loop_thread is not None:
                self._loop_thread.join(timeout=5.0)
            self._loop_thread = None
            self._loop = None
            raise
        # SECURITY FIX 2: Register shared browser user_data_dir with orphan sweeper
        try:
            import flaresolverr_service as fs
            fs.register_shared_browser_dir(self._shared_browser_user_data_dir)
        except Exception as e:
            logger.warning(f"Failed to register shared browser dir: {e}")
        logger.info("ChromeManager: started")

    def _recover(self) -> bool:
        """Full recovery after the event loop crashed. Runs ON the loop thread
        (invoked from _run's except path). Replaces the crashed loop with a fresh
        one (a crashed loop is unreliable for run_coroutine_threadsafe, and all
        async primitives/browser refs are stale), relaunches the shared browser,
        and recreates+re-registers every primary tab via _async_create_tab_registered.
        Coordinated with _restart_lock so it never races restart_browser(). Returns
        True on successful recovery (so _run resets its backoff), False otherwise."""
        if not self._restart_lock.acquire(blocking=False):
            return False  # restart_browser() already recovering
        try:
            # Snapshot primary urls (and reuse their tab_ids for registry continuity).
            with self._lock:
                primaries = [(url, tid) for url, tid in list(self._url_index.items())]
                # Drop registry entries pointing at dead-browser tab objects.
                self._tabs.clear()
                self._url_index.clear()
            # Fresh loop + relaunch + recreate primaries, all pumped here so any
            # async work completes without a separate run_forever.
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            prev_browser = self._browser
            prev_shared_dir = getattr(self, "_shared_browser_user_data_dir", None)

            async def _recover_coro():
                # Force-kill pre-crash browser tree if still running
                if prev_browser is not None:
                    try:
                        pid = getattr(prev_browser, "_process_pid", None)
                        if pid:
                            import signal
                            import psutil
                            proc = psutil.Process(pid)
                            for child in proc.children(recursive=True):
                                try:
                                    child.kill()
                                except Exception:
                                    pass
                            proc.kill()
                    except Exception:
                        pass

                self._browser = await _launch_browser()
                self._shared_browser_user_data_dir = getattr(_launch_browser, "user_data_dir", None)
                # Re-register the shared dir with the orphan sweeper. Unregister
                # the OLD (pre-crash) dir FIRST so a pre-crash Chrome that is still
                # alive can be reaped as an orphan and its profile dir swept
                try:
                    import flaresolverr_service as fs
                    if prev_shared_dir:
                        fs.unregister_shared_browser_dir(prev_shared_dir)
                        if os.path.exists(prev_shared_dir):
                            shutil.rmtree(prev_shared_dir, ignore_errors=True)
                    fs.register_shared_browser_dir(self._shared_browser_user_data_dir)
                except Exception as e:
                    logger.warning(f"ChromeManager: failed to re-register shared dir after recovery: {e}")
                for url, tid in primaries:
                    try:
                        state = await self._async_create_tab_registered(url, tid, is_shadow=False)
                        state.status = "running"
                    except Exception as e:
                        logger.error(f"ChromeManager: failed to recover primary tab for {url}: {e}")

            self._loop.run_until_complete(_recover_coro())
            logger.info("ChromeManager: recovered %d primaries after event loop crash", len(primaries))
            # Legacy metrics: recompute from the recreated registry; the pre-crash
            # session is over, so (re)mark uptime for the recreated primaries.
            self._recompute_ws_gauges(reset_uptime=True)
            return True
        except Exception as e:
            logger.error(f"ChromeManager: recovery failed: {e}")
            return False
        finally:
            self._restart_lock.release()

    def stop(self):
        if not self._running:
            return
        logger.info("ChromeManager: stopping")
        self._running = False  # Signal loop thread to exit its while loop
        # SECURITY FIX 2: Unregister shared browser user_data_dir from orphan sweeper
        try:
            import flaresolverr_service as fs
            fs.unregister_shared_browser_dir(self._shared_browser_user_data_dir)
        except Exception as e:
            logger.warning(f"Failed to unregister shared browser dir: {e}")
        # (1) Close all tabs via _call with SHORT timeout (5s)
        with self._lock:
            ids = list(self._tabs.keys())
        for tid in ids:
            try:
                self._retire_id(tid)
            except Exception as e:
                logger.warning(f"ChromeManager: error retiring tab {tid}: {e}")
        # (2) browser.stop()
        try:
            self._call(self._async_stop(), timeout=5.0)
        except Exception as e:
            logger.warning(f"ChromeManager: browser stop error: {e}")
        # (3) loop.call_soon_threadsafe(loop.stop)
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        # (4) loop_thread.join(timeout=5)
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5.0)
        self._loop_thread = None
        self._loop = None
        # Legacy metrics: recompute from the (now empty) registry so the gauges
        # drop to zero instead of staying inflated for a later start()/recreate.
        self._recompute_ws_gauges()
        # B1: shut down the recycle executor with wait=False
        if self._recycle_executor is not None:
            self._recycle_executor.shutdown(wait=False, cancel_futures=True)
            self._recycle_executor = None
        logger.info("ChromeManager: stopped")

    def restart_browser(self):
        """Full browser process restart: stop current Chrome, launch fresh one,
        and recreate all primary tabs. Used by maintenance on memory pressure.
        Honours _restart_lock so it never races _recover() (loop-crash watchdog):
        if a recovery is already in progress, this is a no-op (the recovery will
        restore primaries)."""
        if not self._restart_lock.acquire(blocking=False):
            logger.info("ChromeManager: restart_browser deferred (recovery already in progress)")
            return
        try:
            logger.info("ChromeManager: restarting browser process")
            # Capture primary URLs before stopping
            with self._lock:
                primaries = [self._tabs[tid].url for tid in self._url_index.values() if tid in self._tabs]
            # Stop current browser
            self.stop()
            # Start fresh
            self.start()
            # Recreate primary tabs
            for url in primaries:
                try:
                    self.create_tab(url)
                except Exception as e:
                    logger.error(f"ChromeManager: failed to recreate primary tab for {url}: {e}")
            # Legacy metrics: final recompute from the registry after recreation.
            self._recompute_ws_gauges()
        finally:
            self._restart_lock.release()

    def create_tab(self, url: str) -> TabState:
        """Create a NEW tab for url. Deliberately does NOT dedup by url: a
        shadow tab for an already-listened url is required for zero-drop handoff.
        The primary for a url is tracked by _url_index."""
        with self._lock:
            if len(self._tabs) >= self.max_tabs * 2:   # allow one shadow per primary
                raise MaxTabsReachedError(f"Max tabs exceeded ({self.max_tabs} primaries + shadows)")
        tab_id = f"tab_{uuid4().hex}"
        with self._lock:
            # A shadow tab (url already has a primary) is allowed beyond the
            # per-URL primary cap, up to a hard total cap (1 shadow per primary).
            # Checked here, BEFORE any browser work, so a rejected create leaks
            # no tab.
            is_shadow = self.get_primary_tab(url) is not None
            if not is_shadow and len(self._url_index) >= self.max_tabs:
                raise MaxTabsReachedError(f"Max tabs ({self.max_tabs}) reached")
            if len(self._tabs) >= self.max_tabs * 2:
                raise MaxTabsReachedError("Max tabs exceeded (primaries + shadow)")
        tab_state = self._call(self._async_create_tab_registered(url, tab_id, is_shadow))
        tab_state.status = "running"
        # Legacy metrics: align gauges with the registry (never blind inc/dec). A
        # shadow (warming) tab re-affirms the per-URL gauge but must NOT reset
        # uptime; only a url becoming a live primary (re)marks the uptime start.
        m.WS_LISTENERS_ACTIVE.set(self._live_primary_count())
        self._sync_url_metrics(url, reset_uptime=not is_shadow)
        logger.info(f"ChromeManager: created tab {tab_id} for {url}")
        return tab_state

    def warm_tab(self, url: str, timeout: float = 15.0, require_frame: bool = True) -> TabState:
        """Create a shadow tab and (when require_frame) wait until it captures its
        first frame. Returns the new shadow; does NOT become primary until
        handoff() runs. On timeout/failure the shadow is retired to avoid leaks.

        require_frame=False skips the first-frame wait (used by ZOMBIE recycles:
        a silent primary produces no shadow frame, so waiting would abort the very
        recycle that should recover it — quorum A5). The shared-browser wedge
        (all listeners silent) is handled separately by the all-stale watchdog."""
        tab_state = self.create_tab(url)
        tab_state.status = "warming"
        if not require_frame:
            tab_state.status = "running"
            logger.info(f"ChromeManager: created warm tab for {url} (no first-frame wait)")
            return tab_state
        start = time.time()
        try:
            while time.time() - start < timeout:
                with tab_state.lock:
                    captured = bool(tab_state.frame_buffer)
                if captured:
                    tab_state.status = "running"
                    logger.info(f"ChromeManager: warmed tab for {url} after {time.time()-start:.1f}s")
                    return tab_state
                time.sleep(0.5)
            raise Exception(f"ChromeManager: tab warm timeout for {url}")
        finally:
            # If we didn't return the tab (timeout or exception), retire it.
            if tab_state.status != "running":
                self.retire_tab_id(tab_state.tab_id)

    # ---- primary resolution (mirrors WebSocketListenerManager._url_index) --

    def get_primary_tab(self, url: str) -> Optional[TabState]:
        """Return the CURRENT primary tab for a url (None if none)."""
        with self._lock:
            tid = self._url_index.get(url)
            if tid is None or tid not in self._tabs:
                return None
            return self._tabs[tid]

    def get_tab(self, url: str) -> Optional[TabState]:
        """Alias of get_primary_tab for the read path. (If no primary yet but a
        tab exists, returns the only one as a courtesy.)"""
        primary = self.get_primary_tab(url)
        if primary is not None:
            return primary
        with self._lock:
            for ts in self._tabs.values():
                if ts.url == url:
                    return ts
        return None

    def ensure_can_create_primary(self, url: str):
        """Raise MaxTabsReachedError if `url` has no primary tab AND the manager
        is already at its max_tabs primary cap — the exact condition create_tab
        enforces for non-shadow creates (a shadow for an already-served url is
        always allowed). Lets callers reject a doomed background boot
        synchronously, before spawning the thread create_tab would fail. NOT a
        reservation: a concurrent create may still claim a slot after this check."""
        if self.get_primary_tab(url) is not None:
            return
        with self._lock:
            if len(self._url_index) >= self.max_tabs:
                raise MaxTabsReachedError(f"Max tabs ({self.max_tabs}) reached")

    def declare_primary(self, url: str, tab_id: str):
        """Set url's primary to tab_id (used by FrameRouter.handoff)."""
        with self._lock:
            self._url_index[url] = tab_id

    def swap_primary(self, url: str, old_tab_id: str, new_tab_id: str) -> bool:
        """Atomically point url at new_tab_id (only if still old_tab_id).

        Returns True if the url still pointed at old_tab_id (and was re-pointed),
        or False if a concurrent restart/recover already re-pointed the url
        elsewhere (a stale-handoff signal; no change made)."""
        with self._lock:
            if self._url_index.get(url) == old_tab_id:
                self._url_index[url] = new_tab_id
                return True
            return False

    def schedule_recycling(self, url: str, reason: str, func, *args):
        """Submit a tab recycle/restart task to the single recycle worker and
        return its future. Public seam for the maintenance loop — callers must
        use this method, NOT `cm._recycle_executor.submit(...)`. All tab ops
        serialize on the single asyncio loop, so the executor is max_workers=1."""
        if self._recycle_executor is None:
            self._recycle_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="tab_recycle")
        return self._recycle_executor.submit(func, *args)

    def get_shared_browser_user_data_dir(self) -> Optional[str]:
        """Return the shared browser's user_data_dir for orphan-sweeper registration."""
        return getattr(self, "_shared_browser_user_data_dir", None)

    def _retire_id(self, tab_id: str):
        with self._lock:
            tab_state = self._tabs.pop(tab_id, None)
        if tab_state is None:
            return
        # Legacy metrics: derive gauges from the registry instead of blind dec().
        # After a handoff the url may still have a live primary, so the per-URL
        # gauge must stay 1 (and uptime continuous); only retiring the LAST tab
        # for a url drops it to 0 and shrinks the global count.
        self._recompute_ws_gauges()
        self._sync_url_metrics(tab_state.url)
        try:
            self._call(self._async_close_tab(tab_state), timeout=10.0)
        except Exception:
            pass
        logger.info(f"ChromeManager: retired tab {tab_id} for {tab_state.url}")

    def retire_tab(self, url: str):
        """Close the PRIMARY tab for url and clear its _url_index entry."""
        with self._lock:
            tid = self._url_index.pop(url, None)
        if tid is not None:
            self._retire_id(tid)

    def retire_tab_id(self, tab_id: str):
        """Close a specific tab by id WITHOUT touching _url_index (used after handoff)."""
        with self._lock:
            # drop any _url_index entries pointing at this id
            for url, cur in list(self._url_index.items()):
                if cur == tab_id:
                    del self._url_index[url]
        self._retire_id(tab_id)

    def drain_tab(self, url: str) -> List[dict]:
        """Drain the PRIMARY tab for url."""
        tab_state = self.get_primary_tab(url)
        if not tab_state:
            return []
        with tab_state.lock:
            frames = list(tab_state.frame_buffer)
            tab_state.frame_buffer.clear()
        # Legacy metrics
        for f in frames:
            ft = f.get("type", "unknown")
            # Legacy WS_MESSAGES_TOTAL uses type labels "received"/"sent" (not the
            # raw CDP method suffix); map them so dashboards stay flat across the
            # flag-on deployment (quorum A3a).
            label = {"webSocketFrameReceived": "received",
                     "webSocketFrameSent": "sent"}.get(ft, ft)
            m.WS_MESSAGES_TOTAL.labels(url=url, type=label).inc(1)
        return frames

    def get_memory_usage_gb(self) -> float:
        """Best-effort Chrome process RSS (browser + children); falls back to 0.
        Inspects Linux cgroup memory if running in a container, or calculates total
        RSS of active and standby Chrome process trees."""
        # Try cgroup v2
        try:
            with open("/sys/fs/cgroup/memory.current", "r") as f:
                val = int(f.read().strip())
                if val > 0:
                    return val / 1e9
        except Exception:
            pass
        # Try cgroup v1
        try:
            with open("/sys/fs/cgroup/memory/memory.usage_in_bytes", "r") as f:
                val = int(f.read().strip())
                if val > 0:
                    return val / 1e9
        except Exception:
            pass

        try:
            import psutil
            total = 0.0
            pids = set()
            for browser_obj in (self._browser, getattr(self, "_standby_browser", None)):
                pid = getattr(browser_obj, "_process_pid", None)
                if pid:
                    pids.add(pid)

            for pid in pids:
                try:
                    proc = psutil.Process(pid)
                    total += proc.memory_info().rss / 1e9
                    for child in proc.children(recursive=True):
                        try:
                            total += child.memory_info().rss / 1e9
                        except Exception:
                            pass
                except Exception:
                    pass
            return total
        except Exception:
            pass
        return 0.0

    def soft_reload_tab(self, tab_id: str, timeout: float = 30.0) -> bool:
        """Synchronously perform a Tier 1 soft reload for the tab."""
        try:
            return self._call(self._async_soft_reload_tab(tab_id), timeout=timeout)
        except Exception as e:
            logger.error(f"ChromeManager: soft_reload_tab failed for {tab_id}: {e}")
            with self._lock:
                tab_state = self._tabs.get(tab_id)
                if tab_state:
                    tab_state.status = "crashed"
            return False

    async def _async_launch_standby_browser(self):
        return await _launch_standby_browser()

    def launch_standby_browser(self, timeout: float = 60.0):
        """Launch an isolated standby Chrome browser instance."""
        return self._call(self._async_launch_standby_browser(), timeout=timeout)

    async def _async_warm_standby_tabs(self, standby_browser, urls: List[str], concurrency: int = 2, timeout: float = 60.0) -> Dict[str, TabState]:
        warmed_tabs: Dict[str, TabState] = {}
        concurrency = max(1, concurrency)

        async def _warm_tab_internal(url: str) -> TabState:
            tab = await standby_browser.get(url)
            tab_id = f"tab_standby_{uuid4().hex}"
            tab_state = TabState(
                tab_id=tab_id,
                url=url,
                tab=tab,
                target_id=getattr(tab, "target_id", ""),
                status="warming",
            )
            await self._async_attach_cdp(tab_state)
            start_ts = time.time()
            tab_timeout = min(timeout, 15.0)
            while time.time() - start_ts < tab_timeout:
                with tab_state.lock:
                    if tab_state.status == "crashed":
                        break
                    has_frame = tab_state.last_data_frame_ts > 0 or bool(tab_state.frame_buffer)
                if has_frame:
                    tab_state.status = "running"
                    break
                await asyncio.sleep(0.1)
            if tab_state.status != "crashed":
                tab_state.status = "running"
            return tab_state

        for i in range(0, len(urls), concurrency):
            batch = urls[i:i + concurrency]
            results = await asyncio.gather(*[_warm_tab_internal(u) for u in batch], return_exceptions=False)
            for ts in results:
                warmed_tabs[ts.url] = ts

        return warmed_tabs

    def warm_standby_tabs(self, standby_browser, urls: List[str], concurrency: int = 2, timeout: float = 60.0) -> Dict[str, TabState]:
        """Synchronously warm tabs in standby browser in batches."""
        return self._call(self._async_warm_standby_tabs(standby_browser, urls, concurrency=concurrency, timeout=timeout), timeout=timeout + 60.0)

    def swap_standby_browser(self, standby_browser, standby_user_data_dir: Optional[str], new_tabs: Dict[str, TabState], router, quiescence_s: float = 2.0):
        """Atomically promote standby browser to primary with zero-drop cross-process merge."""
        start_time = time.time()
        new_tabs_by_url: Dict[str, TabState] = {}
        for val in new_tabs.values():
            if isinstance(val, TabState):
                new_tabs_by_url[val.url] = val

        old_tabs_to_retire = []
        with self._lock:
            for ts in new_tabs_by_url.values():
                self._tabs[ts.tab_id] = ts

            for url, old_tab_id in list(self._url_index.items()):
                new_tab = new_tabs_by_url.get(url)
                if not new_tab:
                    continue
                old_tab = self._tabs.get(old_tab_id)
                if not old_tab:
                    continue
                swapped = self.swap_primary(url, old_tab_id, new_tab.tab_id)
                if swapped:
                    old_tabs_to_retire.append((url, old_tab, new_tab))

            # If there are URLs in new_tabs not in _url_index, register them as primaries
            for url, ts in new_tabs_by_url.items():
                if url not in self._url_index:
                    self._url_index[url] = ts.tab_id

        # Perform cross-process buffer merge & dedup (snapshot buffers under lock, dedup unlocked)
        for url, old_tab, new_tab in old_tabs_to_retire:
            with old_tab.lock:
                with new_tab.lock:
                    old_frames = list(old_tab.frame_buffer)
                    new_frames = list(new_tab.frame_buffer)
                    old_tab.frame_buffer.clear()
                    new_tab.frame_buffer.clear()
                    maxlen = getattr(new_tab.frame_buffer, "maxlen", 2000)

            merged = router._merge_dedup(old_frames, new_frames, maxlen=maxlen, cross_process=True)

            with new_tab.lock:
                new_tab.frame_buffer.extend(merged)
                new_tab.status = "running"
            old_tab.status = "retiring"

        # Quiescence window to drain tail frames from old browser tabs
        if quiescence_s > 0:
            time.sleep(quiescence_s)

        for url, old_tab, new_tab in old_tabs_to_retire:
            tail = []
            with old_tab.lock:
                if old_tab.frame_buffer:
                    tail = list(old_tab.frame_buffer)
                    old_tab.frame_buffer.clear()
            if tail:
                with new_tab.lock:
                    existing = list(new_tab.frame_buffer)
                    maxlen = getattr(new_tab.frame_buffer, "maxlen", 2000)

                # Merge older tail frames before existing newer frames to preserve strict chronological ordering
                merged = router._merge_dedup(tail, existing, maxlen=maxlen, cross_process=True)
                with new_tab.lock:
                    new_tab.frame_buffer.clear()
                    new_tab.frame_buffer.extend(merged)

        # Remove old tabs from registry
        with self._lock:
            for _, old_tab, _ in old_tabs_to_retire:
                self._tabs.pop(old_tab.tab_id, None)

        # Clean up old browser process & profile
        old_browser = self._browser
        old_user_data_dir = getattr(self, "_shared_browser_user_data_dir", None)
        if old_browser is not None:
            try:
                pid = getattr(old_browser, "_process_pid", None)
                if pid:
                    import psutil
                    proc = psutil.Process(pid)
                    for child in proc.children(recursive=True):
                        try:
                            child.kill()
                        except Exception:
                            pass
                    proc.kill()
            except Exception:
                try:
                    stop = getattr(old_browser, "stop", None)
                    if callable(stop):
                        stop()
                except Exception as e:
                    logger.warning(f"ChromeManager: old browser stop error: {e}")

        if old_user_data_dir:
            try:
                import flaresolverr_service as fs
                fs.unregister_shared_browser_dir(old_user_data_dir)
            except Exception as e:
                logger.warning(f"ChromeManager: error unregistering old shared dir: {e}")
            try:
                if os.path.exists(old_user_data_dir):
                    shutil.rmtree(old_user_data_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"ChromeManager: error removing old user_data_dir: {e}")

        # Promote standby browser
        self._browser = standby_browser
        self._shared_browser_user_data_dir = standby_user_data_dir or getattr(standby_browser, "user_data_dir", None)

        # Metrics & gauges
        duration = time.time() - start_time
        m.WS_STANDBY_HANDOFF_DURATION.observe(duration)
        self._recompute_ws_gauges()

    def recycle_browser_standby(self, router, reason: str = "scheduled", quiescence_s: float = 2.0) -> bool:
        """Synchronously execute full standby browser swap: launch, warm, swap, and cleanup."""
        if not self._restart_lock.acquire(blocking=False):
            logger.info("ChromeManager: recycle_browser_standby skipped (restart/recover in progress)")
            return False
        standby_browser = None
        standby_dir = None
        try:
            logger.info(f"ChromeManager: starting standby browser recycle (reason={reason})")
            with self._lock:
                urls = list(self._url_index.keys())
            if not urls:
                logger.info("ChromeManager: no active tabs to migrate to standby browser")
                return False
            standby_browser = self.launch_standby_browser()
            self._standby_browser = standby_browser
            standby_dir = getattr(standby_browser, "user_data_dir", None)
            new_tabs = self.warm_standby_tabs(standby_browser, urls)
            self.swap_standby_browser(standby_browser, standby_dir, new_tabs, router, quiescence_s=quiescence_s)
            self._standby_browser = None
            logger.info(f"ChromeManager: standby browser recycle completed (reason={reason})")
            return True
        except Exception as e:
            logger.error(f"ChromeManager: standby browser recycle failed: {e}")
            if standby_browser is not None:
                try:
                    pid = getattr(standby_browser, "_process_pid", None)
                    if pid:
                        import psutil
                        proc = psutil.Process(pid)
                        for child in proc.children(recursive=True):
                            try:
                                child.kill()
                            except Exception:
                                pass
                        proc.kill()
                except Exception:
                    try:
                        stop = getattr(standby_browser, "stop", None)
                        if callable(stop):
                            stop()
                    except Exception:
                        pass
            if standby_dir:
                try:
                    import flaresolverr_service as fs
                    fs.unregister_shared_browser_dir(standby_dir)
                except Exception:
                    pass
                try:
                    if os.path.exists(standby_dir):
                        shutil.rmtree(standby_dir, ignore_errors=True)
                except Exception:
                    pass
            raise
        finally:
            self._standby_browser = None
            self._restart_lock.release()


async def warm_standby_tabs(standby_browser, urls: List[str], concurrency: int = 2, timeout: float = 60.0, chrome_manager=None) -> Dict[str, TabState]:
    """Top-level function for warming standby tabs."""
    cm = chrome_manager or ChromeManager()
    return await cm._async_warm_standby_tabs(standby_browser, urls, concurrency=concurrency, timeout=timeout)


def swap_standby_browser(standby_browser, standby_user_data_dir, new_tabs: Dict[str, TabState], router, chrome_manager=None, quiescence_s: float = 2.0):
    """Top-level function for swapping standby browser."""
    cm = chrome_manager or getattr(router, "mgr", None)
    if cm is None:
        raise ValueError("chrome_manager required for swap_standby_browser")
    return cm.swap_standby_browser(standby_browser, standby_user_data_dir, new_tabs, router, quiescence_s=quiescence_s)


def recycle_browser_standby(router, reason: str = "scheduled", chrome_manager=None, quiescence_s: float = 2.0):
    """Top-level function for recycling browser using standby instance."""
    cm = chrome_manager or getattr(router, "mgr", None)
    if cm is None:
        raise ValueError("chrome_manager required for recycle_browser_standby")
    return cm.recycle_browser_standby(router, reason=reason, quiescence_s=quiescence_s)
