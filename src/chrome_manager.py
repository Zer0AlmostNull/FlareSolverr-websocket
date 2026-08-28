import asyncio
import logging
import threading
import time
import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List
from uuid import uuid4
import tempfile

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
    # Unique user_data_dir per manager instance so profile sweepers don't
    # mistake the shared browser for an orphan.
    user_data_dir = tempfile.mkdtemp(prefix="tabmgr_")
    # SECURITY FIX 1: sandbox=True by default; allow override via WS_CHROME_SANDBOX env var
    # (default true). Only disable if Cloudflare validation proves it blocks.
    import os
    sandbox = os.environ.get('WS_CHROME_SANDBOX', 'true').lower() == 'true'
    browser = await uc.start(
        headless=headless,
        sandbox=sandbox,
        user_data_dir=user_data_dir,
        browser_args=[
            "--no-first-run",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1920,1080",
            # SECURITY FIX 2: site-per-process for renderer isolation.
            # nodriver's Config emits `--disable-features=IsolateOrigins,site-per-process`
            # by default, which would cancel an explicit --site-per-process. Explicitly
            # re-enable both features so per-site process isolation actually holds.
            "--site-per-process",
            "--enable-features=IsolateOrigins,site-per-process",
        ],
    )
    # Store user_data_dir for orphan-sweeper registration
    _launch_browser.user_data_dir = user_data_dir
    return browser


@dataclass
class TabState:
    tab_id: str
    url: str
    tab: object                            # nodriver Tab (asyncio-bound)
    target_id: str
    frame_buffer: deque = field(default_factory=lambda: deque(maxlen=utils.get_config_ws_listener_default_max_msgs()))
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_frame_ts: float = field(default_factory=time.time)
    status: str = "starting"             # starting | running | warming | handoff | retiring
    service_started_at: datetime = field(default_factory=datetime.now)
    handlers: list = field(default_factory=list)   # registered CDP handler refs


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
        await tab.send(uc.cdp.network.enable())

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

        tab.add_handler(uc.cdp.network.WebSocketFrameReceived, on_received)
        tab.add_handler(uc.cdp.network.WebSocketFrameSent, on_sent)
        tab_state.handlers = [on_received, on_sent]

    async def _async_close_tab(self, tab_state: TabState):
        try:
            await tab_state.tab.close()
        except Exception:
            logger.debug(f"close tab {tab_state.url} (already gone)")

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
        return fut.result(timeout=timeout)

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
            prev_shared_dir = getattr(self, "_shared_browser_user_data_dir", None)

            async def _recover_coro():
                self._browser = await _launch_browser()
                self._shared_browser_user_data_dir = getattr(_launch_browser, "user_data_dir", None)
                # Re-register the shared dir with the orphan sweeper. Unregister
                # the OLD (pre-crash) dir FIRST so a pre-crash Chrome that is still
                # alive can be reaped as an orphan and its profile dir swept
                # (quorum A6/C12 — previously the old dir stayed registered
                # forever and leaked both the process and the profile dir).
                try:
                    import flaresolverr_service as fs
                    if prev_shared_dir:
                        fs.unregister_shared_browser_dir(prev_shared_dir)
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
        # B1: shut down the recycle executor with wait=False (NOT wait=True — a
        # worker blocked in _call on the shutting-down loop can stall up to
        # LOOP_CALL_TIMEOUT/120s). Null it so a later start() re-creates it.
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
                primaries = {url: self._tabs[tid].url for url, tid in self._url_index.items()}
            # Stop current browser
            self.stop()
            # Start fresh
            self.start()
            # Recreate primary tabs
            for url in primaries.values():
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
        self._call(self._async_close_tab(tab_state))
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

        nodriver's Browser exposes no public `pid`; the real Chrome OS pid is the
        private `_process_pid` (set on launch). Reading /proc/self/statm would
        measure the FlareSolverr PYTHON process, not Chrome, so we deliberately do
        NOT fall back to it — a wrong reading would both blind the memory-pressure
        restart and misfire it off unrelated Python growth."""
        try:
            import psutil
            pid = getattr(self._browser, "_process_pid", None)
            if pid is not None:
                proc = psutil.Process(pid)
                total = proc.memory_info().rss / 1e9
                for child in proc.children(recursive=True):
                    try:
                        total += child.memory_info().rss / 1e9
                    except Exception:
                        pass
                return total
        except Exception:
            pass
        return 0.0


# TabState._feed: thread-safe frame append (called from asyncio thread)
def _tab_feed(self, frame_type: str, payload: str, cdp_ts=None):
    # SECURITY FIX 3: max frame size guard (1MB)
    MAX_FRAME_SIZE = 1_000_000
    if len(payload) > MAX_FRAME_SIZE:
        logger.warning(f"Dropping oversized WS frame ({len(payload)} bytes) for {self.url}")
        return
    msg = {
        "timestamp": time.time(),
        "type": frame_type,                      # "webSocketFrameReceived" | "webSocketFrameSent"
        "url": self.url,
        "payload": payload,
        "cdp_ts": cdp_ts,                        # CDP MonotonicTime; INTERNAL (stripped before /websocket_messages return)
    }
    with self.lock:
        self.frame_buffer.append(msg)
        self.last_frame_ts = time.time()


TabState._feed = _tab_feed
