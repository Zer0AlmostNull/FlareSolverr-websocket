import asyncio
import gc
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta

import certifi
import undetected_chromedriver as uc
from bottle import run, response, Bottle, request, ServerAdapter

from bottle_plugins.error_plugin import error_plugin
from bottle_plugins.logger_plugin import logger_plugin
from bottle_plugins import prometheus_plugin
from chrome_manager import MaxTabsReachedError
from dtos import V1RequestBase, HealthResponse, STATUS_OK # Added HealthResponse, STATUS_OK
import flaresolverr_service
import metrics
import utils

logger = logging.getLogger(__name__)

env_proxy_url = os.environ.get('PROXY_URL', None)
env_proxy_username = os.environ.get('PROXY_USERNAME', None)
env_proxy_password = os.environ.get('PROXY_PASSWORD', None)

SESSION_HEALTH_CHECK_INTERVAL = int(os.environ.get("SESSION_HEALTH_CHECK_INTERVAL", 60)) # seconds

ws_listener_manager = flaresolverr_service.WebSocketListenerManager(
    max_listeners=utils.get_config_max_ws_listeners())

# Lazy singleton for the single-Chrome TabManager. Constructed on first request
# when WS_TAB_MANAGER_ENABLED=true (NOT at import time: avoids spawning Chrome
# during module import and makes the flag testable via request-time env).
_tab_mgr = {"cm": None, "router": None, "service": None}
# Guards the lazy singleton construction: the first endpoint request can race
# the first _tab_manager_maintenance() tick, and a plain ``is None`` check lets
# BOTH build+start() a ChromeManager — the loser is dropped from the singleton
# but its user_data_dir stays registered with the orphan sweeper forever.
# Double-checked init under this lock keeps exactly one ChromeManager alive.
_tab_mgr_init_lock = threading.Lock()

# Module-scoped per-URL recycle cooldown: prevents the same URL being recycled
# twice in the same tick (e.g. zombie watchdog AND max-lifetime both firing)
# from spawning a leaked running shadow. Mirrors the legacy
# WebSocketListenerManager._recycle_cooldown_until.
_recycle_cooldown_until: dict = {}

# All-stale shared-browser watchdog: when EVERY primary has been silent this long
# the shared Chrome is presumed dead/hung (per-URL recycle cannot help) and the
# whole browser is restarted. Cooldown prevents a hot restart loop when sites
# merely fall quiet (each restart re-creates fresh tabs which are then silent).
STALE_RESTART_WINDOW_S = 120.0
BROWSER_RESTART_COOLDOWN_S = 600.0
_browser_restart_cooldown_until = 0.0
_restart_cooldown_until = 0.0

WS_DATA_STALL_TIMEOUT_S = 60.0
GMGN_GRACE_PERIOD_S = 180.0
WS_CHROME_RECYCLE_INTERVAL_HOURS = 6.0
WS_CHROME_MAX_MEMORY_MB = 1200
_last_periodic_recycle_time = time.time()


def _manager_broken(cm) -> bool:
    """True if a constructed ChromeManager can no longer serve (its dedicated
    event loop died or was wound down — e.g. a restarted browser failed to
    launch). Returns False when no manager was constructed yet (nothing to heal),
    so an injected/test singleton is never discarded for being merely absent."""
    if cm is None:
        return False  # not constructed; nothing to heal
    if not getattr(cm, "_running", False):
        return True
    loop = getattr(cm, "_loop", None)
    if loop is None:
        return True
    return loop.is_closed() or not loop.is_running()


def _ensure_tab_manager():
    """Return the TabManager service, or None if the flag is off (use legacy)."""
    # Self-heal: a cached manager whose loop has died (e.g. a failed
    # restart_browser that wound the loop down) can no longer serve — discard it
    # here so the next construction below rebuilds a fresh, working manager
    # instead of returning a permanently-wedged singleton (quorum C3/C1).
    if _manager_broken(_tab_mgr["cm"]):
        _reset_tab_manager()
    if _tab_mgr["service"] is not None:
        return _tab_mgr["service"]
    if not utils.get_config_ws_tab_manager_enabled():
        return None
    with _tab_mgr_init_lock:
        # Re-check inside the lock: a concurrent thread may have rebuilt it.
        if _manager_broken(_tab_mgr["cm"]):
            _reset_tab_manager()
        if _tab_mgr["service"] is not None:
            return _tab_mgr["service"]
        from chrome_manager import ChromeManager
        from frame_router import FrameRouter
        from frame_router_service import FrameRouterService
        cm = ChromeManager(max_tabs=utils.get_config_max_ws_listeners())
        router = FrameRouter(cm)
        service = FrameRouterService(cm, router)
        cm.start()
        _tab_mgr["cm"], _tab_mgr["router"], _tab_mgr["service"] = cm, router, service
        return service


def _reset_tab_manager():
    _tab_mgr["cm"] = None
    _tab_mgr["router"] = None
    _tab_mgr["service"] = None


def _live(tab) -> bool:
    """A tab is 'live' (serving) when it is in an active/reloading state and not retired or crashed."""
    if tab is None:
        return False
    return getattr(tab, "status", "") in ("starting", "warming", "handoff", "running", "reloading")


def _release_url_metrics(m, url: str):
    """Drop the per-URL legacy gauges for a url no longer served (so retired urls
    do not leave stale values in /metrics output — mirrors legacy
    WebSocketListenerManager._release_url_metrics)."""
    for gauge in (m.WS_LISTENER_ACTIVE, m.WS_LISTENER_UPTIME, m.WS_LISTENER_LAST_SEEN):
        try:
            gauge.remove(url)
        except KeyError:
            pass


class JSONErrorBottle(Bottle):
    """
    Handle 404 errors
    """
    def default_error_handler(self, res):
        response.content_type = 'application/json'
        return json.dumps(dict(error=res.body, status_code=res.status_code))


app = JSONErrorBottle()


@app.route('/')
def index():
    """
    Show welcome message
    """
    res = flaresolverr_service.index_endpoint()
    return utils.object_to_dict(res)


@app.route('/health')
def health():
    """
    Healthcheck endpoint.
    This endpoint is special because it doesn't print traces
    """
    res = flaresolverr_service.health_endpoint()
    return utils.object_to_dict(res)


@app.post('/v1')
def controller_v1():
    """
    Controller v1
    """
    data = request.json or {}
    if (('proxy' not in data or not data.get('proxy')) and env_proxy_url is not None and (env_proxy_username is None and env_proxy_password is None)):
        logging.info('Using proxy URL ENV')
        data['proxy'] = {"url": env_proxy_url}
    if (('proxy' not in data or not data.get('proxy')) and env_proxy_url is not None and (env_proxy_username is not None or env_proxy_password is not None)):
        logging.info('Using proxy URL, username & password ENVs')
        data['proxy'] = {"url": env_proxy_url, "username": env_proxy_username, "password": env_proxy_password}
    req = V1RequestBase(data)
    res = flaresolverr_service.controller_v1_endpoint(req)
    if res.__error_500__:
        response.status = 500
    return utils.object_to_dict(res)

@app.get('/websocket_messages')
def get_websocket_messages():
    """
    Ensures a WebSocket listener exists for the given URL (creating it in the
    background on first use) and returns its collected messages (drained).

    Response: {"status": "starting|running|unhealthy|failed", "messages": [...]}
    """
    url = request.query.get('url')
    if not url:
        response.status = 400
        response.content_type = 'application/json'
        return json.dumps({"error": "Parameter 'url' is required."})
    tab_service = _ensure_tab_manager()
    if tab_service is not None:
        try:
            payload = tab_service.ensure_and_fetch(url)
        except MaxTabsReachedError:
            response.status = 429
            response.content_type = 'application/json'
            return json.dumps({"error": "Max listeners reached"})
    else:
        try:
            payload = ws_listener_manager.ensure_and_fetch(url)
        except flaresolverr_service.MaxListenersReachedError:
            response.status = 429
            response.content_type = 'application/json'
            return json.dumps({"error": "Max listeners reached"})
    response.content_type = 'application/json'
    return json.dumps(payload)

def update_lifecycle_gauges():
    """Export thread/object-census gauges so thread-pool or driver accumulation
    is visible in Prometheus instead of rediscovered via archaeology. O(heap)
    census — safe at the background-sweep cadence, never per-request."""
    try:
        metrics.PROCESS_THREADS_ACTIVE.set(threading.active_count())
        metrics.THREAD_POOL_WORKERS.set(sum(
            1 for t in threading.enumerate()
            if t.name.startswith('ThreadPoolExecutor-')))
        metrics.GC_EVENT_LOOPS.set(sum(
            1 for o in gc.get_objects()
            if isinstance(o, asyncio.AbstractEventLoop)))
        metrics.GC_CHROME_DRIVERS.set(sum(
            1 for o in gc.get_objects()
            if isinstance(o, uc.Chrome)))
        metrics.UNQUIT_CHROME_DRIVERS.set(len(uc.LIVE_CHROMES))
        try:
            with open('/proc/self/statm') as f:
                rss_pages = int(f.read().split()[1])
            metrics.PROCESS_RSS_BYTES.set(rss_pages * os.sysconf('SC_PAGE_SIZE'))
        except Exception:
            # Non-Linux or restricted /proc: report 0 rather than skipping the sample
            metrics.PROCESS_RSS_BYTES.set(0)
    except Exception as e:
        logging.error(f"Error updating lifecycle gauges: {e}")


def _recycle_tab(cm, router, url, reason):
    """Warm a NEW shadow tab, then hand off. handoff() re-points the primary
    and closes the old tab. Never raises out of the maintenance caller (runs on
    the recycle worker). Single-flighted against restart/recover via
    cm._restart_lock and per-URL cooldowned (mirrors the legacy recycle lock +
    cooldown) so the same tick cannot double-submit the same URL."""
    import metrics as m
    import time
    new_tab = None
    start = time.time()
    # Single-flight: never recycle while a restart/recover is in progress (it
    # will be re-evaluated next tick), and serialize recycles globally.
    if not cm._restart_lock.acquire(blocking=False):
        logger.info(f"TabManager: recycle for {url} skipped (restart/recover in progress)")
        return
    try:
        now = datetime.now()
        if now < _recycle_cooldown_until.get(url, datetime.min):
            logger.info(f"TabManager: recycle for {url} in cooldown; skipping")
            return
        # Arm the cooldown BEFORE doing work; cleared only on success.
        _recycle_cooldown_until[url] = now + timedelta(minutes=10)

        old_tab = cm.get_tab(url)
        if old_tab is None:
            # No primary yet; just ensure one exists (first-use path).
            cm.create_tab(url)
            m.WS_TAB_HANDOFF_TOTAL.labels(url=url, result="first_create").inc()
            return
        new_tab = cm.warm_tab(url, require_frame=(reason != "zombie"))
        result = router.handoff(url, old_tab, new_tab)
        # handoff returns "stale" when a concurrent restart/recover re-pointed
        # the url (the shadow was retired by handoff, no data lost).
        label = result if result == "stale" else "success"
        m.WS_TAB_HANDOFF_TOTAL.labels(url=url, result=label).inc()
        m.WS_HANDOFF_DURATION.labels(url=url).observe(time.time() - start)
        _recycle_cooldown_until.pop(url, None)
    except Exception as e:
        logger.error(f"TabManager recycle failed for {url}: {e}")
        m.WS_TAB_HANDOFF_TOTAL.labels(url=url, result="failed").inc()
        m.WS_HANDOFF_DURATION.labels(url=url).observe(time.time() - start)
    finally:
        # Retire the shadow UNLESS it is now the url's primary (handoff
        # succeeded). The old status-based guard (`status != "running"`) failed
        # to catch a leak: warm_tab leaves the shadow "running", so an exception
        # raised by router.handoff kept it registered forever. Retiring an
        # already-retired id (e.g. handoff's stale path) is a safe no-op, so we
        # can always retire when the shadow is not the current primary.
        if new_tab is not None and cm.get_primary_tab(url) is not new_tab:
            cm.retire_tab_id(new_tab.tab_id)
        cm._restart_lock.release()


def _restart_all_tabs(cm, router, reason):
    import metrics as m
    try:
        logger.warning(f"TabManager: full browser restart triggered ({reason})")
        cm.restart_browser()  # restart_browser() itself honours _restart_lock
        m.WS_TAB_RESTART_TOTAL.labels(reason=reason).inc()
    except Exception as e:
        logger.error(f"TabManager full restart failed: {e}")
        m.WS_TAB_RESTART_TOTAL.labels(reason=f"{reason}_failed").inc()


def _recycle_browser_standby(cm_or_reason, router=None, reason: str = "scheduled"):
    """Recycle shared Chrome using isolated standby browser with zero-drop handoff."""
    import metrics as m
    global _browser_restart_cooldown_until, _restart_cooldown_until
    if router is None and isinstance(cm_or_reason, str):
        reason = cm_or_reason
        cm = _tab_mgr["cm"]
        router = _tab_mgr["router"]
    else:
        cm = cm_or_reason
    if cm is None or router is None:
        logger.warning("TabManager: _recycle_browser_standby skipped (cm or router is None)")
        return
    try:
        logger.warning(f"TabManager: standby browser recycle triggered ({reason})")
        cm.recycle_browser_standby(router, reason=reason)
        m.WS_STANDBY_BROWSER_RECYCLES.labels(reason=reason).inc()
        now = time.time()
        _browser_restart_cooldown_until = now + BROWSER_RESTART_COOLDOWN_S
        _restart_cooldown_until = _browser_restart_cooldown_until
    except Exception as e:
        logger.error(f"TabManager standby browser recycle failed: {e}")


def _tab_manager_maintenance():
    """Maintenance tick for TabManager: 3-tier stall escalation, scheduled/memory
    triggers, max-lifetime recycle, and observability gauges."""
    global _browser_restart_cooldown_until, _restart_cooldown_until, _last_periodic_recycle_time
    if _ensure_tab_manager() is None:
        return
    cm = _tab_mgr["cm"]
    router = _tab_mgr["router"]
    now = time.time()

    with cm._lock:
        urls = list(getattr(cm, "_url_index", {}).keys())
        primaries = {url: cm._tabs[tid] for url, tid in list(cm._url_index.items())
                     if tid in getattr(cm, "_tabs", {})}

    # All-stale shared-browser watchdog fallback: when ALL primaries are missing/silent >120s
    if urls and not primaries and all(
        (ts := router.get_last_frame_ts(u)) is None or now - ts > STALE_RESTART_WINDOW_S
        for u in urls
    ):
        if now >= _browser_restart_cooldown_until:
            logger.warning("TabManager: ALL listeners stale; shared Chrome presumed dead; restarting browser")
            _browser_restart_cooldown_until = now + BROWSER_RESTART_COOLDOWN_S
            _restart_cooldown_until = _browser_restart_cooldown_until
            cm.schedule_recycling(urls[0], "all_stale", _restart_all_tabs, cm, router, "all_stale")
        return

    # 1. 3-Tier Escalation Engine for Data Stalls
    data_stall_timeout = float(os.environ.get("WS_DATA_STALL_TIMEOUT_S", str(WS_DATA_STALL_TIMEOUT_S)))
    gmgn_grace_period = GMGN_GRACE_PERIOD_S

    stalled_tabs = []
    for url in urls:
        tab = primaries.get(url)
        if tab is None or not _live(tab):
            ts = router.get_last_frame_ts(url)
            if ts and now - ts > data_stall_timeout:
                cm.schedule_recycling(url, "zombie", _recycle_tab, cm, router, url, "zombie")
            continue

        tab_age = (datetime.now() - tab.service_started_at).total_seconds()
        is_gmgn = "gmgn.ai" in url.lower() or "gmgn" in url.lower()

        # Check GMGN grace period for fresh GMGN tabs
        if is_gmgn and tab_age < gmgn_grace_period:
            continue

        # Check if tab has stalled data frames
        if getattr(tab, "last_data_frame_ts", 0.0) > 0:
            is_stalled = (now - tab.last_data_frame_ts) > data_stall_timeout
        else:
            is_stalled = tab_age > data_stall_timeout

        if is_stalled:
            stalled_tabs.append((url, tab))
        else:
            with tab.lock:
                tab.consecutive_stalls = 0

    import metrics as m

    # Multi-tab stall escalation -> immediate Tier 3
    if len(stalled_tabs) >= 2:
        for url, tab in stalled_tabs:
            m.WS_STALL_ESCALATIONS.labels(url=url, tier="3").inc()
            with tab.lock:
                tab.consecutive_stalls = max(getattr(tab, "consecutive_stalls", 0), 3)
        if now >= _browser_restart_cooldown_until:
            logger.warning(f"TabManager: multiple tabs stalled ({[u for u, _ in stalled_tabs]}); escalating to Tier 3 standby recycle")
            _browser_restart_cooldown_until = now + BROWSER_RESTART_COOLDOWN_S
            _restart_cooldown_until = _browser_restart_cooldown_until
            cm.schedule_recycling(stalled_tabs[0][0], "stall_tier3", _recycle_browser_standby, cm, router, "stall_tier3")
    elif len(stalled_tabs) == 1:
        url, tab = stalled_tabs[0]
        with tab.lock:
            stalls = getattr(tab, "consecutive_stalls", 0)
        if stalls == 0:
            # Tier 1: Soft reload
            m.WS_STALL_ESCALATIONS.labels(url=url, tier="1").inc()
            reload_ok = cm.soft_reload_tab(tab.tab_id)
            with tab.lock:
                if reload_ok:
                    tab.consecutive_stalls = 1
                    logger.info(f"TabManager: Tier 1 soft reload succeeded for {url}")
                else:
                    # Reload failed -> escalate directly to Tier 2
                    logger.warning(f"TabManager: Tier 1 soft reload failed for {url}; escalating to Tier 2")
                    m.WS_STALL_ESCALATIONS.labels(url=url, tier="2").inc()
                    tab.consecutive_stalls = 2
            if not reload_ok:
                cm.schedule_recycling(url, "stall_tier2", _recycle_tab, cm, router, url, "stall_tier2")
        elif stalls == 1:
            # Tier 2: Per-tab recycle
            m.WS_STALL_ESCALATIONS.labels(url=url, tier="2").inc()
            with tab.lock:
                tab.consecutive_stalls = 2
            logger.warning(f"TabManager: Tier 2 tab recycle triggered for {url}")
            cm.schedule_recycling(url, "stall_tier2", _recycle_tab, cm, router, url, "stall_tier2")
        else:
            # Tier 3: Standby browser recycle (consecutive_stalls >= 2)
            m.WS_STALL_ESCALATIONS.labels(url=url, tier="3").inc()
            with tab.lock:
                tab.consecutive_stalls += 1
            if now >= _browser_restart_cooldown_until:
                logger.warning(f"TabManager: Tier 3 standby recycle triggered for {url}")
                _browser_restart_cooldown_until = now + BROWSER_RESTART_COOLDOWN_S
                _restart_cooldown_until = _browser_restart_cooldown_until
                cm.schedule_recycling(url, "stall_tier3", _recycle_browser_standby, cm, router, "stall_tier3")

    # 2. Memory-triggered recycling and emergency fallback
    mem = cm.get_memory_usage_gb()
    max_memory_mb = float(os.environ.get("WS_CHROME_MAX_MEMORY_MB", str(WS_CHROME_MAX_MEMORY_MB)))
    emergency_memory_mb = float(os.environ.get("WS_EMERGENCY_MEMORY_MB", "1400"))
    max_memory_gb = max_memory_mb / 1024.0
    emergency_memory_gb = emergency_memory_mb / 1024.0

    if mem > emergency_memory_gb:
        # Emergency fallback: Fast in-place restart (bypasses routine 600s cooldown)
        logger.warning(f"TabManager EMERGENCY memory {mem:.2f}GB > {emergency_memory_gb:.2f}GB; performing fast in-place restart")
        _browser_restart_cooldown_until = now + 120.0
        _restart_cooldown_until = _browser_restart_cooldown_until
        cm.schedule_recycling(next(iter(urls), ""), "emergency_memory", _restart_all_tabs, cm, router, "emergency_memory")
    elif mem > max_memory_gb:
        # Chrome RSS ceiling: Standby browser zero-drop recycle
        if now >= _browser_restart_cooldown_until:
            logger.warning(f"TabManager memory {mem:.2f}GB > ceiling ({max_memory_mb}MB); recycling via standby browser")
            _browser_restart_cooldown_until = now + BROWSER_RESTART_COOLDOWN_S
            _restart_cooldown_until = _browser_restart_cooldown_until
            cm.schedule_recycling(next(iter(urls), ""), "memory", _recycle_browser_standby, cm, router, "memory")

    # 3. Scheduled periodic maintenance (default 6-hour timer)
    recycle_interval_hours = float(os.environ.get("WS_CHROME_RECYCLE_INTERVAL_HOURS", str(WS_CHROME_RECYCLE_INTERVAL_HOURS)))
    if recycle_interval_hours > 0:
        periodic_interval_s = recycle_interval_hours * 3600.0
        if now - _last_periodic_recycle_time >= periodic_interval_s:
            _last_periodic_recycle_time = now
            if now >= _browser_restart_cooldown_until:
                logger.info(f"TabManager periodic {recycle_interval_hours}h timer triggered standby recycle")
                _browser_restart_cooldown_until = now + BROWSER_RESTART_COOLDOWN_S
                _restart_cooldown_until = _browser_restart_cooldown_until
                cm.schedule_recycling(next(iter(urls), ""), "scheduled", _recycle_browser_standby, cm, router, "scheduled")

    # 4. Max-lifetime restart (WS_LISTENER_MAX_LIFETIME_MINUTES, default 180)
    max_life_s = utils.get_config_ws_listener_max_lifetime() * 60
    if max_life_s > 0:
        for url in urls:
            ts = cm.get_primary_tab(url)
            if ts is None:
                continue
            age = (datetime.now() - ts.service_started_at).total_seconds()
            if age > max_life_s:
                logger.info(f"TabManager max-lifetime reached for {url}; recycling")
                cm.schedule_recycling(url, "max_lifetime", _recycle_tab, cm, router, url, "max_lifetime")

    # 5. Observability Gauges
    with cm._lock:
        tabs = list(getattr(cm, "_tabs", {}).values())
        primaries = {url: cm._tabs[tid] for url, tid in list(cm._url_index.items())
                     if tid in getattr(cm, "_tabs", {})}
    m.WS_TABS_ACTIVE.set(len(tabs))
    m.WS_TABS_RUNNING.set(sum(1 for t in tabs if getattr(t, "status", "") == "running"))
    now_dt = datetime.now()
    for t in tabs:
        age = (now_dt - t.service_started_at).total_seconds() if hasattr(t, "service_started_at") else 0.0
        m.WS_TAB_AGE.labels(url=t.url, tab_id=t.tab_id).set(age)
        maxlen = getattr(t.frame_buffer, "maxlen", 2000) if hasattr(t, "frame_buffer") else 2000
        if maxlen > 0 and hasattr(t, "frame_buffer"):
            m.WS_FRAME_BUFFER_UTILIZATION.labels(url=t.url).set(len(t.frame_buffer) / maxlen)
        if hasattr(t, "data_frame_rate"):
            m.WS_DATA_FRAME_RATE.labels(url=t.url).set(t.data_frame_rate(60.0))
        if hasattr(t, "control_frame_rate"):
            m.WS_CONTROL_FRAME_RATE.labels(url=t.url).set(t.control_frame_rate(60.0))

    # Legacy WS_* gauge parity
    running = 0
    status_counts = {}
    for url, primary in primaries.items():
        if _live(primary):
            running += 1
            st = primary.status
            status_counts[st] = status_counts.get(st, 0) + 1
            m.WS_LISTENER_ACTIVE.labels(url=url).set(1)
            m.WS_LISTENER_UPTIME.labels(url=url).set(
                (now_dt - primary.service_started_at).total_seconds())
            m.WS_LISTENER_LAST_SEEN.labels(url=url).set(now_dt.timestamp())
        else:
            m.WS_LISTENER_ACTIVE.labels(url=url).set(0)
            _release_url_metrics(m, url)
    m.WS_LISTENERS_ACTIVE.set(len(primaries))
    m.WS_LISTENERS_RUNNING.set(running)
    for st in ("starting", "running", "unhealthy"):
        m.WS_LISTENERS_STATUS.labels(status=st).set(status_counts.get(st, 0))
    m.WS_LOOP_THREAD_ALIVE.set(1 if getattr(cm, "_loop_thread", None) and cm._loop_thread.is_alive() else 0)


def background_tasks_thread():
    last_cleanup = 0
    CLEANUP_INTERVAL = 600

    while True:
        now = time.time()
        if now - last_cleanup >= CLEANUP_INTERVAL:
            try:
                logging.debug("Triggering periodic stale sessions cleanup...")
                flaresolverr_service.SESSIONS_STORAGE.cleanup_stale_sessions()
            except Exception as e:
                logging.error(f"Error during stale sessions cleanup: {e}")
            last_cleanup = now

        try:
            ws_listener_manager.cleanup_stale()
        except Exception as e:
            logging.error(f"Error during ws listener cleanup: {e}")

        try:
            _tab_manager_maintenance()
        except Exception as e:
            logging.error(f"Error during tab manager maintenance: {e}")

        live = set()
        try:
            live = flaresolverr_service._live_user_data_dirs()
            utils.kill_orphaned_chrome(live)
        except Exception as e:
            logging.error(f"Error during orphaned chrome cleanup: {e}")

        try:
            removed = utils.sweep_stale_profile_dirs(live)
            if removed:
                logging.info("profile dir sweep removed %d stale dirs", removed)
        except Exception as e:
            logging.error(f"Error during profile dir sweep: {e}")

        try:
            update_lifecycle_gauges()
        except Exception as e:
            logging.error(f"Error in lifecycle gauges: {e}")

        try:
            if utils.get_config_enable_periodic_gc():
                gc.collect()
        except Exception as e:
            logging.error(f"periodic gc failed: {e}")

        time.sleep(SESSION_HEALTH_CHECK_INTERVAL)

if __name__ == "__main__":
    # check python version
    if sys.version_info < (3, 9):
        raise Exception("The Python version is less than 3.9, a version equal to or higher is required.")

    # fix for HEADLESS=false in Windows binary
    # https://stackoverflow.com/a/27694505
    if os.name == 'nt':
        import multiprocessing
        multiprocessing.freeze_support()

    # fix ssl certificates for compiled binaries
    # https://github.com/pyinstaller/pyinstaller/issues/7229
    # https://stackoverflow.com/q/55736855
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
    os.environ["SSL_CERT_FILE"] = certifi.where()

    # validate configuration
    log_level = os.environ.get('LOG_LEVEL', 'info').upper()
    log_file = os.environ.get('LOG_FILE', None)
    log_html = utils.get_config_log_html()
    headless = utils.get_config_headless()
    server_host = os.environ.get('HOST', '0.0.0.0')
    server_port = int(os.environ.get('PORT', 8191))

    # configure logger
    logger_format = '%(asctime)s %(levelname)-8s %(message)s'
    if log_level == 'DEBUG':
        logger_format = '%(asctime)s %(levelname)-8s ReqId %(thread)s %(message)s'
    if log_file:
        log_file = os.path.realpath(log_file)
        log_path = os.path.dirname(log_file)
        os.makedirs(log_path, exist_ok=True)
        logging.basicConfig(
            format=logger_format,
            level=log_level,
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(log_file)
            ]
        )
    else:
        logging.basicConfig(
            format=logger_format,
            level=log_level,
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )

    # disable warning traces from urllib3
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
    logging.getLogger('undetected_chromedriver').setLevel(logging.WARNING)

    logging.info(f'FlareSolverr {utils.get_flaresolverr_version()}')
    logging.debug('Debug log enabled')

    # Get current OS for global variable
    utils.get_current_platform()

    # test browser installation
    flaresolverr_service.test_browser_installation()

    # start bootle plugins
    # plugin order is important
    app.install(logger_plugin)
    app.install(error_plugin)
    prometheus_plugin.setup()
    app.install(prometheus_plugin.prometheus_plugin)

    # start webserver
    # default server 'wsgiref' does not support concurrent requests
    # https://github.com/FlareSolverr/FlareSolverr/issues/680
    # https://github.com/Pylons/waitress/issues/31
    class WaitressServerPoll(ServerAdapter):
        def run(self, handler):
            from waitress import serve
            serve(handler, host=self.host, port=self.port, asyncore_use_poll=True)
    
    # Start background tasks (session cleanup, reload, and health checker)
    background_thread = threading.Thread(target=background_tasks_thread, daemon=True)
    background_thread.start()
    logging.info("Background tasks thread started.")

    run(app, host=server_host, port=server_port, quiet=True, server=WaitressServerPoll)
