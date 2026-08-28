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
    """A tab is 'live' (serving) when it is not being retired/closed."""
    return tab is not None and tab.status not in ("retiring",)


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


def _tab_manager_maintenance():
    """Maintenance tick for the TabManager: zombie watchdog, memory-triggered
    restart, and max-lifetime recycle, plus tab/loop gauges. No-op when the
    flag is off. Recycle/restart work is offloaded to the recycle executor so
    this never blocks the background sweep loop."""
    global _browser_restart_cooldown_until
    if _ensure_tab_manager() is None:
        return
    cm = _tab_mgr["cm"]
    router = _tab_mgr["router"]
    now = time.time()
    # urls are the KEYS of _url_index (primaries); _tabs is id-keyed. Snapshot
    # both under cm._lock (RLock): the loop thread and recycle worker mutate
    # these dicts under the same lock, so an unlocked list() can raise
    # `RuntimeError: dictionary changed size during iteration`.
    with cm._lock:
        urls = list(getattr(cm, "_url_index", {}).keys())

    # 1) Zombie watchdog. Two distinct failure modes:
    #    a) ALL primaries silent -> the SHARED Chrome is likely dead/hung. A
    #       per-URL recycle can't help (warming a shadow on a dead browser fails),
    #       so restart the whole browser — but ONLY when every listener is stale,
    #       never for a single silent tab (that is the per-URL path below, and is
    #       also protected by the recycle cooldown to avoid churn).
    #    b) A single primary silent >60s -> recycle just that tab (zero-drop
    #       handoff to a fresh shadow keeps serving that url).
    if urls and all(
        (ts := router.get_last_frame_ts(u)) is None or now - ts > STALE_RESTART_WINDOW_S
        for u in urls
    ):
        if now >= _browser_restart_cooldown_until:
            logger.warning("TabManager: ALL listeners stale; shared Chrome presumed dead; restarting browser")
            _browser_restart_cooldown_until = now + BROWSER_RESTART_COOLDOWN_S
            cm.schedule_recycling(urls[0], "all_stale", _restart_all_tabs, cm, router, "all_stale")
    else:
        for url in urls:
            ts = router.get_last_frame_ts(url)
            if ts and now - ts > 60:
                logger.warning(f"TabManager zombie detected for {url}; recycling")
                cm.schedule_recycling(url, "zombie", _recycle_tab, cm, router, url, "zombie")

    # 2) Memory-triggered restart (single-flight, offloaded to recycle worker so
    #    a slow Chrome restart never blocks the background sweep loop).
    mem = cm.get_memory_usage_gb()
    if mem > 1.2:
        if now >= _browser_restart_cooldown_until:
            logger.warning(f"TabManager memory {mem:.2f}GB >1.2; restarting all tabs")
            _browser_restart_cooldown_until = now + BROWSER_RESTART_COOLDOWN_S
            cm.schedule_recycling(next(iter(urls), ""), "memory", _restart_all_tabs, cm, router, "memory")

    # 3) Max-lifetime restart (WS_LISTENER_MAX_LIFETIME_MINUTES, default 180).
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

    # 4) Gauges.
    import metrics as m
    with cm._lock:
        tabs = list(cm._tabs.values())
        primaries = {url: cm._tabs[tid] for url, tid in list(cm._url_index.items())
                     if tid in cm._tabs}
    m.WS_TABS_ACTIVE.set(len(tabs))
    m.WS_TABS_RUNNING.set(sum(1 for t in tabs if t.status == "running"))
    now_dt = datetime.now()
    for t in tabs:
        age = (now_dt - t.service_started_at).total_seconds()
        m.WS_TAB_AGE.labels(url=t.url, tab_id=t.tab_id).set(age)
        maxlen = getattr(t.frame_buffer, "maxlen", 2000)
        if maxlen > 0:
            m.WS_FRAME_BUFFER_UTILIZATION.labels(url=t.url).set(len(t.frame_buffer) / maxlen)

    # Legacy WS_* gauge parity for the new path (quorum A3). Legacy maintains
    # UPTIME as continuous elapsed seconds, LAST_SEEN as the heartbeat timestamp,
    # and RUNNING/STATUS as the per-url status census; recompute them here so the
    # flag-on deployment keeps dashboards accurate.
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
    m.WS_LOOP_THREAD_ALIVE.set(1 if cm._loop_thread and cm._loop_thread.is_alive() else 0)


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
