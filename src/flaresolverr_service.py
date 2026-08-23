import logging
import platform
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from html import escape
from urllib.parse import unquote, quote
from uuid import uuid4

from func_timeout import FunctionTimedOut, func_timeout
from selenium.common import TimeoutException
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.expected_conditions import (
    presence_of_element_located, staleness_of, title_is)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait

import utils
from metrics import (
    WS_LISTENERS_RUNNING,
    WS_LISTENERS_ACTIVE, WS_LISTENERS_STATUS, WS_LISTENERS_TOTAL,
    WS_RECONNECT_TOTAL, WS_MESSAGES_TOTAL, WS_SESSION_DURATION,
    WS_LISTENER_ACTIVE, WS_LISTENER_UPTIME, WS_LISTENER_TOTAL_ACTIVE, WS_LISTENER_LAST_SEEN,
)
from dtos import (STATUS_ERROR, STATUS_OK, ChallengeResolutionResultT,
                  ChallengeResolutionT, HealthResponse, IndexResponse,
                  V1RequestBase, V1ResponseBase)
from sessions import SessionsStorage

ACCESS_DENIED_TITLES = [
    # Cloudflare
    'Access denied',
    # Cloudflare http://bitturk.net/ Firefox
    'Attention Required! | Cloudflare'
]
ACCESS_DENIED_SELECTORS = [
    # Cloudflare
    'div.cf-error-title span.cf-code-label span',
    # Cloudflare http://bitturk.net/ Firefox
    '#cf-error-details div.cf-error-overview h1'
]
CHALLENGE_TITLES = [
    # Cloudflare
    'Just a moment...',
    # DDoS-GUARD
    'DDoS-Guard'
]
CHALLENGE_SELECTORS = [
    # Cloudflare
    '#cf-challenge-running', '.ray_id', '.attack-box', '#cf-please-wait', '#challenge-spinner', '#trk_jschal_js', '#turnstile-wrapper', '.lds-ring',
    # Custom CloudFlare for EbookParadijs, Film-Paleis, MuziekFabriek and Puur-Hollands
    'td.info #js_info',
    # Fairlane / pararius.com
    'div.vc div.text-box h2'
]

TURNSTILE_SELECTORS = [
    "input[name='cf-turnstile-response']"
]

SHORT_TIMEOUT = 1
SESSIONS_STORAGE = SessionsStorage()

class MaxListenersReachedError(Exception):
    pass


@dataclass
class WebsocketMessage:
    timestamp: float
    type: str  # "sent" or "received"
    url: str
    payload: str


@dataclass
class WebSocketListener:
    listener_id: str
    session_id: str = ""
    url: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime = field(default_factory=datetime.now)
    ttl_minutes: int = 30
    max_messages: int = 500
    status: str = "starting"  # starting | running | unhealthy | failed
    error_message: str = ""
    reconnect_attempts: int = 0

# WEBSOCKET_MESSAGES: deque[WebsocketMessage] = deque(maxlen=500)

def _live_user_data_dirs() -> set:
    from flaresolverr import ws_listener_manager
    dirs = set()
    for session in SESSIONS_STORAGE.sessions.values():
        d = getattr(session.driver, "_fs_user_data_dir", None)
        if d:
            dirs.add(d)
    for listener in ws_listener_manager.listeners.values():
        if listener.session_id:
            session = SESSIONS_STORAGE.sessions.get(listener.session_id)
            if session:
                d = getattr(session.driver, "_fs_user_data_dir", None)
                if d:
                    dirs.add(d)
    return dirs


def _websocket_message_handler(session, event, log_level=logging.INFO, track_metrics: bool = True):
    params = event['params']
    frame_type = event['method'].split('.')[-1]  # "webSocketFrameReceived" or "webSocketFrameSent"

    # Extract relevant information
    now = time.time()
    # Cache the URL for 5 seconds to avoid excessive current_url calls
    if now - session.url_cache_time > 5:
        try:
            session.last_known_url = session.driver.current_url
            session.url_cache_time = now
        except Exception:
            pass

    url = params.get('url') or getattr(session, 'target_url', '') or session.last_known_url
    payload = params['response']['payloadData'] if 'response' in params and 'payloadData' in params['response'] else \
              params.get('payloadData', '') # Fallback to empty string if not found

    websocket_msg = WebsocketMessage(
        timestamp=time.time(),
        type=frame_type,
        url=url,
        payload=payload
    )
    session.websocket_messages.append(websocket_msg)
    logging.log(log_level, f"Websocket message {frame_type}: {len(payload.encode('utf-8'))} bytes")
    if track_metrics:
        frame_type_label = "received" if frame_type == "webSocketFrameReceived" else "sent"
        WS_MESSAGES_TOTAL.labels(url=url, type=frame_type_label).inc()


def _register_listener_cdp(session):
    driver = session.driver
    driver.execute_cdp_cmd("Network.enable", {})

    def _handler(event, _session=session):
        _websocket_message_handler(_session, event, log_level=logging.DEBUG)

    driver.add_cdp_listener("Network.webSocketFrameReceived", _handler)
    driver.add_cdp_listener("Network.webSocketFrameSent", _handler)
    return _handler


class WebSocketListenerManager:
    def __init__(self, max_listeners: int = 5):
        self.listeners: dict[str, WebSocketListener] = {}
        self.max_listeners = max_listeners
        self._url_index: dict[str, str] = {}
        self._lock = threading.Lock()

    def _get_session(self, listener: WebSocketListener):
        return SESSIONS_STORAGE.sessions.get(listener.session_id)

    def _pop_url_index(self, url: str, listener_id: str):
        """Remove the index entry ONLY if it belongs to this listener.
        If this listener owned the entry but another live listener still
        serves the URL, re-point the index to it instead of clearing.
        Must be called while holding self._lock."""
        if self._url_index.get(url) != listener_id:
            return
        fallback = next(
            (lid for lid, l in self.listeners.items() if l.url == url), None)
        if fallback is not None:
            self._url_index[url] = fallback
        else:
            del self._url_index[url]

    def create_listener(self, url: str, ttl_minutes: int = None,
                        max_messages: int = None) -> WebSocketListener:
        if not url:
            raise ValueError("Field 'url' is mandatory.")
        with self._lock:
            if len(self.listeners) >= self.max_listeners:
                WS_LISTENERS_TOTAL.labels(event="max_reached").inc()
                raise MaxListenersReachedError(
                    f"Max listeners reached ({self.max_listeners})")
            ttl_minutes = ttl_minutes or utils.get_config_ws_listener_default_ttl()
            max_messages = max_messages or utils.get_config_ws_listener_default_max_msgs()
            listener_id = str(uuid4())
            now = datetime.now()
            listener = WebSocketListener(
                listener_id=listener_id,
                session_id="",
                url=url,
                created_at=now,
                last_heartbeat=now,
                ttl_minutes=ttl_minutes,
                max_messages=max_messages,
                status="starting",
            )
            self.listeners[listener_id] = listener
            self._url_index[url] = listener_id
            WS_LISTENERS_TOTAL.labels(event="created").inc()
            self._update_gauges()
        # Browser work (Chrome launch + page load) happens OFF the lock.
        # _start_session cleans up and re-raises on failure.
        self._start_session(listener, url, max_messages)
        return listener

    def _start_session(self, listener: WebSocketListener, url: str, max_messages: int):
        try:
            session, _ = SESSIONS_STORAGE.create(
                session_id=f"ws_listener_{listener.listener_id}", target_url=url)
            session.websocket_messages = deque(maxlen=max_messages)
            _register_listener_cdp(session)
            create_timeout = utils.get_config_ws_listener_create_timeout()
            try:
                func_timeout(create_timeout, session.driver.get, (url,))
            except FunctionTimedOut:
                raise Exception(f"Timed out loading {url} after {create_timeout}s")
            with self._lock:
                if listener.listener_id in self.listeners:
                    listener.session_id = session.session_id
                    listener.status = "running"
                    listener.error_message = ""
            self._update_gauges()
            self._update_per_url_metrics()
        except Exception as e:
            logging.error(f"Failed to start WS listener {listener.listener_id} for {url}: {e}")
            with self._lock:
                self.listeners.pop(listener.listener_id, None)
                self._pop_url_index(url, listener.listener_id)
                listener.status = "failed"
                listener.error_message = str(e)
            try:
                SESSIONS_STORAGE.destroy(f"ws_listener_{listener.listener_id}")
            except Exception:
                pass
            utils.kill_orphaned_chrome(_live_user_data_dirs())
            self._release_url_metrics(url)
            self._update_gauges()
            raise

    def ensure_listener_for_url(self, url: str, ttl_minutes: int = None,
                                max_messages: int = None) -> WebSocketListener:
        if not url:
            raise ValueError("Field 'url' is mandatory.")
        with self._lock:
            if url in self._url_index:
                lid = self._url_index[url]
                listener = self.listeners.get(lid)
                if listener is not None:
                    listener.last_heartbeat = datetime.now()
                    return listener
            if len(self.listeners) >= self.max_listeners:
                WS_LISTENERS_TOTAL.labels(event="max_reached").inc()
                raise MaxListenersReachedError(
                    f"Max listeners reached ({self.max_listeners})")
            ttl_minutes = ttl_minutes or utils.get_config_ws_listener_default_ttl()
            max_messages = max_messages or utils.get_config_ws_listener_default_max_msgs()
            listener_id = str(uuid4())
            now = datetime.now()
            listener = WebSocketListener(
                listener_id=listener_id,
                session_id="",
                url=url,
                created_at=now,
                last_heartbeat=now,
                ttl_minutes=ttl_minutes,
                max_messages=max_messages,
                status="starting",
            )
            self.listeners[listener_id] = listener
            self._url_index[url] = listener_id
            WS_LISTENERS_TOTAL.labels(event="created").inc()
            self._update_gauges()
        t = threading.Thread(target=self._start_session,
                             args=(listener, url, max_messages), daemon=True)
        t.start()
        return listener

    def _locked_get(self, listener_id: str):
        with self._lock:
            listener = self.listeners.get(listener_id)
            if listener is not None:
                now = datetime.now()
                listener.last_heartbeat = now
                WS_LISTENER_LAST_SEEN.labels(url=listener.url).set(now.timestamp())
            return listener

    def _drain(self, listener_id: str):
        with self._lock:
            listener = self.listeners.get(listener_id)
            if listener is None:
                return None
            if not listener.session_id:
                return []
            session = SESSIONS_STORAGE.sessions.get(listener.session_id)
            if session is None:
                return []
            messages = [msg.__dict__ for msg in list(session.websocket_messages)]
            session.websocket_messages.clear()
            return messages

    def get_listener_payload(self, listener_id: str):
        listener = self._locked_get(listener_id)
        if listener is None:
            return None
        messages = self._drain(listener_id)
        return {
            "status": listener.status,
            "messages": messages or [],
        }

    def ensure_and_fetch(self, url: str):
        listener = self.ensure_listener_for_url(url)
        return self.get_listener_payload(listener.listener_id)

    def get_listener(self, listener_id: str) -> WebSocketListener | None:
        return self._locked_get(listener_id)

    def _update_gauges(self):
        WS_LISTENERS_ACTIVE.set(len(self.listeners))
        # New: count only "running" status listeners
        running_count = sum(1 for l in self.listeners.values() if l.status == "running")
        WS_LISTENERS_RUNNING.set(running_count)
        
        status_counts = {"starting": 0, "running": 0, "unhealthy": 0}
        for listener in list(self.listeners.values()):
            if listener.status in status_counts:
                status_counts[listener.status] += 1
        for status, count in status_counts.items():
            WS_LISTENERS_STATUS.labels(status=status).set(count)

    def _update_per_url_metrics(self):
        now = datetime.now()
        # group listeners by url
        by_url: dict[str, WebSocketListener] = {}
        for lst in list(self.listeners.values()):
            # keep the most recently created listener per url as "primary"
            if lst.url not in by_url or lst.created_at > by_url[lst.url].created_at:
                by_url[lst.url] = lst
        for url, lst in by_url.items():
            is_active = lst.status in ("running", "starting")
            WS_LISTENER_ACTIVE.labels(url=url).set(1 if is_active else 0)
            WS_LISTENER_LAST_SEEN.labels(url=url).set(now.timestamp())
            if is_active:
                WS_LISTENER_UPTIME.labels(url=url).set(
                    (now - lst.created_at).total_seconds())

    def _release_url_metrics(self, url: str):
        """Drop or refresh per-URL gauges after a listener serving `url` is gone."""
        with self._lock:
            still_listened = any(l.url == url for l in self.listeners.values())
        if still_listened:
            self._update_per_url_metrics()
            return
        for gauge in (WS_LISTENER_ACTIVE, WS_LISTENER_UPTIME, WS_LISTENER_LAST_SEEN):
            try:
                gauge.remove(url)
            except KeyError:
                pass

    def get_listener(self, listener_id: str) -> WebSocketListener | None:
        return self._locked_get(listener_id)

    def list_listeners(self) -> list[WebSocketListener]:
        return list(self.listeners.values())

    def destroy_listener(self, listener_id: str) -> bool:
        with self._lock:
            listener = self.listeners.pop(listener_id, None)
            if listener is None:
                return False
            self._pop_url_index(listener.url, listener_id)
        try:
            SESSIONS_STORAGE.destroy(listener.session_id)
        except Exception as e:
            logging.warning(f"Error destroying session for listener {listener_id}: {e}")
        WS_LISTENERS_TOTAL.labels(event="destroyed").inc()
        WS_SESSION_DURATION.labels(url=listener.url).observe(
            (datetime.now() - listener.created_at).total_seconds())
        WS_LISTENER_TOTAL_ACTIVE.labels(url=listener.url).inc(
            (datetime.now() - listener.created_at).total_seconds())
        self._release_url_metrics(listener.url)
        self._update_gauges()
        return True

    def cleanup_stale(self):
        now = datetime.now()
        for listener_id, listener in list(self.listeners.items()):
            # A listener still in 'starting' has no session attached yet
            # (background _start_session is mid-flight). Never reap it here.
            if not listener.session_id:
                continue
            if (now - listener.last_heartbeat) > timedelta(minutes=listener.ttl_minutes):
                logging.info(f"Listener {listener_id} inactive for "
                             f"{listener.ttl_minutes} min. Destroying.")
                WS_LISTENERS_TOTAL.labels(event="ttl_expired").inc()
                self.destroy_listener(listener_id)
                continue
            session = self._get_session(listener)
            if session is None:
                logging.warning(f"Listener {listener_id}: session missing. Destroying.")
                WS_LISTENERS_TOTAL.labels(event="session_missing").inc()
                self.destroy_listener(listener_id)
                continue
            try:
                _ = session.driver.current_url
                if listener.status != "running":
                    listener.status = "running"
                    listener.error_message = ""
                listener.reconnect_attempts = 0
            except Exception as e:
                listener.status = "unhealthy"
                listener.error_message = str(e)
                if listener.reconnect_attempts >= 3:
                    logging.warning(f"Listener {listener_id}: max reconnect attempts "
                                    f"({listener.reconnect_attempts}) reached, destroying.")
                    self.destroy_listener(listener_id)
                else:
                    self._reconnect_listener(listener)
        self._update_gauges()
        self._update_per_url_metrics()

    def _reconnect_listener(self, listener: WebSocketListener):
        listener.reconnect_attempts += 1
        try:
            # Preserve buffered-but-unpolled messages across the crash.
            old_session = SESSIONS_STORAGE.sessions.get(listener.session_id)
            preserved = list(old_session.websocket_messages) if old_session else []
            SESSIONS_STORAGE.destroy(listener.session_id)
            session, _ = SESSIONS_STORAGE.create(
                session_id=listener.session_id, target_url=listener.url)
            session.websocket_messages = deque(
                preserved, maxlen=listener.max_messages)
            _register_listener_cdp(session)
            create_timeout = utils.get_config_ws_listener_create_timeout()
            try:
                func_timeout(create_timeout, session.driver.get, (listener.url,))
            except FunctionTimedOut:
                raise Exception(
                    f"Timed out loading {listener.url} after {create_timeout}s")
            if listener.listener_id in self.listeners:
                listener.status = "running"
                listener.error_message = ""
                listener.reconnect_attempts = 0
                WS_RECONNECT_TOTAL.labels(url=listener.url, result="success").inc()
            else:
                SESSIONS_STORAGE.destroy(listener.session_id)
                WS_RECONNECT_TOTAL.labels(url=listener.url, result="failed").inc()
                WS_LISTENERS_TOTAL.labels(event="max_reconnect_reached").inc()
        except Exception as e:
            listener.error_message = f"Reconnect failed: {e}"
            WS_RECONNECT_TOTAL.labels(url=listener.url, result="failed").inc()


def test_browser_installation():
    logging.info("Testing web browser installation...")
    logging.info("Platform: " + platform.platform())

    chrome_exe_path = utils.get_chrome_exe_path()
    if chrome_exe_path is None:
        logging.error("Chrome / Chromium web browser not installed!")
        sys.exit(1)
    else:
        logging.info("Chrome / Chromium path: " + chrome_exe_path)

    chrome_major_version = utils.get_chrome_major_version()
    if chrome_major_version == '':
        logging.error("Chrome / Chromium version not detected!")
        sys.exit(1)
    else:
        logging.info("Chrome / Chromium major version: " + chrome_major_version)

    logging.info("Launching web browser...")
    user_agent = utils.get_user_agent()
    logging.info("FlareSolverr User-Agent: " + user_agent)

    # Test actual Chrome launch
    try:
        driver = utils.get_webdriver()
        driver.get("data:text/html,<html>test</html>")
        assert "test" in driver.page_source
        driver.quit()
        logging.info("Chrome launch test successful!")
    except Exception as e:
        logging.error(f"Chrome launch test failed: {e}")
        sys.exit(1)

    logging.info("Test successful!")


def index_endpoint() -> IndexResponse:
    res = IndexResponse({})
    res.msg = "FlareSolverr is ready!"
    res.version = utils.get_flaresolverr_version()
    res.userAgent = utils.get_user_agent()
    return res


def health_endpoint() -> HealthResponse:
    res = HealthResponse({})
    res.status = STATUS_OK
    return res


def controller_v1_endpoint(req: V1RequestBase) -> V1ResponseBase:
    start_ts = int(time.time() * 1000)
    logging.info(f"Incoming request => POST /v1 body: {utils.object_to_dict(req)}")
    res: V1ResponseBase
    try:
        res = _controller_v1_handler(req)
    except Exception as e:
        res = V1ResponseBase({})
        res.__error_500__ = True
        res.status = STATUS_ERROR
        res.message = "Error: " + str(e)
        logging.error(res.message)

    res.startTimestamp = start_ts
    res.endTimestamp = int(time.time() * 1000)
    res.version = utils.get_flaresolverr_version()
    logging.debug(f"Response => POST /v1 body: {utils.object_to_dict(res)}")
    logging.info(f"Response in {(res.endTimestamp - res.startTimestamp) / 1000} s")
    return res


def _controller_v1_handler(req: V1RequestBase) -> V1ResponseBase:
    # do some validations
    if req.cmd is None:
        raise Exception("Request parameter 'cmd' is mandatory.")
    if req.headers is not None:
        logging.warning("Request parameter 'headers' was removed in FlareSolverr v2.")
    if req.userAgent is not None:
        logging.warning("Request parameter 'userAgent' was removed in FlareSolverr v2.")

    # set default values
    if req.maxTimeout is None or int(req.maxTimeout) < 1:
        req.maxTimeout = 60000

    # execute the command
    res: V1ResponseBase
    if req.cmd == 'sessions.create':
        res = _cmd_sessions_create(req)
    elif req.cmd == 'sessions.list':
        res = _cmd_sessions_list(req)
    elif req.cmd == 'sessions.destroy':
        res = _cmd_sessions_destroy(req)
    elif req.cmd == 'request.get':
        res = _cmd_request_get(req)
    elif req.cmd == 'request.post':
        res = _cmd_request_post(req)
    else:
        raise Exception(f"Request parameter 'cmd' = '{req.cmd}' is invalid.")

    return res


def _cmd_request_get(req: V1RequestBase) -> V1ResponseBase:
    # do some validations
    if req.url is None:
        raise Exception("Request parameter 'url' is mandatory in 'request.get' command.")
    if req.postData is not None:
        raise Exception("Cannot use 'postBody' when sending a GET request.")
    if req.returnRawHtml is not None:
        logging.warning("Request parameter 'returnRawHtml' was removed in FlareSolverr v2.")
    if req.download is not None:
        logging.warning("Request parameter 'download' was removed in FlareSolverr v2.")

    challenge_res = _resolve_challenge(req, 'GET')
    res = V1ResponseBase({})
    res.status = challenge_res.status
    res.message = challenge_res.message
    res.solution = challenge_res.result
    return res


def _cmd_request_post(req: V1RequestBase) -> V1ResponseBase:
    # do some validations
    if req.postData is None:
        raise Exception("Request parameter 'postData' is mandatory in 'request.post' command.")
    if req.returnRawHtml is not None:
        logging.warning("Request parameter 'returnRawHtml' was removed in FlareSolverr v2.")
    if req.download is not None:
        logging.warning("Request parameter 'download' was removed in FlareSolverr v2.")

    challenge_res = _resolve_challenge(req, 'POST')
    res = V1ResponseBase({})
    res.status = challenge_res.status
    res.message = challenge_res.message
    res.solution = challenge_res.result
    return res


def _cmd_sessions_create(req: V1RequestBase) -> V1ResponseBase:
    logging.debug("Creating new session...")

    session, fresh = SESSIONS_STORAGE.create(session_id=req.session, proxy=req.proxy)
    session_id = session.session_id

    if not fresh:
        return V1ResponseBase({
            "status": STATUS_OK,
            "message": "Session already exists.",
            "session": session_id
        })

    return V1ResponseBase({
        "status": STATUS_OK,
        "message": "Session created successfully.",
        "session": session_id
    })


def _cmd_sessions_list(req: V1RequestBase) -> V1ResponseBase:
    session_ids = SESSIONS_STORAGE.session_ids()

    return V1ResponseBase({
        "status": STATUS_OK,
        "message": "",
        "sessions": session_ids
    })


def _cmd_sessions_destroy(req: V1RequestBase) -> V1ResponseBase:
    session_id = req.session
    existed = SESSIONS_STORAGE.destroy(session_id)

    if not existed:
        raise Exception("The session doesn't exist.")

    return V1ResponseBase({
        "status": STATUS_OK,
        "message": "The session has been removed."
    })


def _resolve_challenge(req: V1RequestBase, method: str) -> ChallengeResolutionT:
    timeout = int(req.maxTimeout) / 1000
    driver = None
    try:
        session = None
        if req.session:
            session_id = req.session
            ttl = timedelta(minutes=req.session_ttl_minutes) if req.session_ttl_minutes else None
            session, fresh = SESSIONS_STORAGE.get(session_id, ttl)

            if fresh:
                logging.debug(f"new session created to perform the request (session_id={session_id})")
            else:
                logging.debug(f"existing session is used to perform the request (session_id={session_id}, "
                              f"lifetime={str(session.lifetime())}, ttl={str(ttl)})")

            driver = session.driver
        else:
            try:
                driver = utils.get_webdriver(req.proxy)
            except Exception as e:
                utils.kill_orphaned_chrome(_live_user_data_dirs())
                raise Exception('Error solving the challenge. ' + str(e).replace('\n', '\\n'))
            logging.debug('New instance of webdriver has been created to perform the request')

        # Enable CDP Network domain and listen for websocket frames
        _session_websocket_message_handler_received = None
        _session_websocket_message_handler_sent = None
        try:
            # We only want to listen for websocket messages if a session is provided
            if session:
                driver.execute_cdp_cmd("Network.enable", {})
                _session_websocket_message_handler_received = lambda event: _websocket_message_handler(session, event, track_metrics=False)
                _session_websocket_message_handler_sent = lambda event: _websocket_message_handler(session, event, track_metrics=False)
                driver.add_cdp_listener("Network.webSocketFrameReceived", _session_websocket_message_handler_received)
                driver.add_cdp_listener("Network.webSocketFrameSent", _session_websocket_message_handler_sent)
                logging.debug("CDP websocket frame listeners added.")
        except Exception as e:
            logging.warning(f"Failed to enable CDP Network domain or add websocket listeners: {e}")

        return func_timeout(timeout, _evil_logic, (req, driver, method))
    except FunctionTimedOut:
        raise Exception(f'Error solving the challenge. Timeout after {timeout} seconds.')
    except Exception as e:
        raise Exception('Error solving the challenge. ' + str(e).replace('\n', '\\n'))
    finally:
        # Disable CDP Network domain and remove listeners
        try:
            if session:
                if _session_websocket_message_handler_received:
                    driver.remove_cdp_listener("Network.webSocketFrameReceived", _session_websocket_message_handler_received)
                if _session_websocket_message_handler_sent:
                    driver.remove_cdp_listener("Network.webSocketFrameSent", _session_websocket_message_handler_sent)
                driver.execute_cdp_cmd("Network.disable", {})
                logging.debug("CDP websocket frame listeners removed and Network domain disabled.")
        except Exception as e:
            logging.warning(f"Failed to disable CDP Network domain or remove websocket listeners: {e}")

        if not req.session and driver is not None:
            if utils.PLATFORM_VERSION == "nt":
                driver.close()
            driver.quit()
            logging.debug('A used instance of webdriver has been destroyed')

def click_verify(driver: WebDriver, num_tabs: int = 1):
    try:
        logging.debug("Try to find the Cloudflare verify checkbox...")
        actions = ActionChains(driver)
        actions.pause(5)
        for _ in range(num_tabs):
            actions.send_keys(Keys.TAB).pause(0.1)
        actions.pause(1)
        actions.send_keys(Keys.SPACE).perform()
        
        logging.debug(f"Cloudflare verify checkbox clicked after {num_tabs} tabs!")
    except Exception:
        logging.debug("Cloudflare verify checkbox not found on the page.")
    finally:
        driver.switch_to.default_content()

    try:
        logging.debug("Try to find the Cloudflare 'Verify you are human' button...")
        button = driver.find_element(
            by=By.XPATH,
            value="//input[@type='button' and @value='Verify you are human']",
        )
        if button:
            actions = ActionChains(driver)
            actions.move_to_element_with_offset(button, 5, 7)
            actions.click(button)
            actions.perform()
            logging.debug("The Cloudflare 'Verify you are human' button found and clicked!")
    except Exception:
        logging.debug("The Cloudflare 'Verify you are human' button not found on the page.")

    time.sleep(2)

def _get_turnstile_token(driver: WebDriver, tabs: int):
    token_input = driver.find_element(By.CSS_SELECTOR, "input[name='cf-turnstile-response']")
    current_value = token_input.get_attribute("value")
    while True:
        click_verify(driver, num_tabs=tabs)
        turnstile_token = token_input.get_attribute("value")
        if turnstile_token:
            if turnstile_token != current_value:
                logging.info(f"Turnstile token: {turnstile_token}")
                return turnstile_token
        logging.debug(f"Failed to extract token possibly click failed")        

        # reset focus
        driver.execute_script("""
            let el = document.createElement('button');
            el.style.position='fixed';
            el.style.top='0';
            el.style.left='0';
            document.body.prepend(el);
            el.focus();
        """)
        time.sleep(1)

def _resolve_turnstile_captcha(req: V1RequestBase, driver: WebDriver):
    turnstile_token = None
    if req.tabs_till_verify is not None:
        logging.debug(f'Navigating to... {req.url} in order to pass the turnstile challenge')
        driver.get(req.url)

        turnstile_challenge_found = False
        for selector in TURNSTILE_SELECTORS:
            found_elements = driver.find_elements(By.CSS_SELECTOR, selector)   
            if len(found_elements) > 0:
                turnstile_challenge_found = True
                logging.info("Turnstile challenge detected. Selector found: " + selector)
                break
        if turnstile_challenge_found:
            turnstile_token = _get_turnstile_token(driver=driver, tabs=req.tabs_till_verify)
        else:
            logging.debug(f'Turnstile challenge not found')
    return turnstile_token

def _evil_logic(req: V1RequestBase, driver: WebDriver, method: str) -> ChallengeResolutionT:
    res = ChallengeResolutionT({})
    res.status = STATUS_OK
    res.message = ""
    turnstile_token = None

    # optionally block resources like images/css/fonts using CDP
    disable_media = utils.get_config_disable_media()
    if req.disableMedia is not None:
        disable_media = req.disableMedia
    if disable_media:
        block_urls = [
            # Images
            "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.bmp", "*.svg", "*.ico",
            "*.PNG", "*.JPG", "*.JPEG", "*.GIF", "*.WEBP", "*.BMP", "*.SVG", "*.ICO",
            "*.tiff", "*.tif", "*.jpe", "*.apng", "*.avif", "*.heic", "*.heif",
            "*.TIFF", "*.TIF", "*.JPE", "*.APNG", "*.AVIF", "*.HEIC", "*.HEIF",
            # Stylesheets
            "*.css",
            "*.CSS",
            # Fonts
            "*.woff", "*.woff2", "*.ttf", "*.otf", "*.eot",
            "*.WOFF", "*.WOFF2", "*.TTF", "*.OTF", "*.EOT"
        ]
        try:
            logging.debug("Network.setBlockedURLs: %s", block_urls)
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Network.setBlockedURLs", {"urls": block_urls})
        except Exception:
            # if CDP commands are not available or fail, ignore and continue
            logging.debug("Network.setBlockedURLs failed or unsupported on this webdriver")

    # navigate to the page
    logging.debug(f"Navigating to... {req.url}")
    if method == "POST":
        _post_request(req, driver)
    else:
        if req.tabs_till_verify is None:
            driver.get(req.url)
        else:
            turnstile_token = _resolve_turnstile_captcha(req, driver)

    # set cookies if required
    if req.cookies is not None and len(req.cookies) > 0:
        logging.debug(f'Setting cookies...')
        for cookie in req.cookies:
            driver.delete_cookie(cookie['name'])
            driver.add_cookie(cookie)
        # reload the page
        if method == 'POST':
            _post_request(req, driver)
        else:
            driver.get(req.url)

    # wait for the page
    if utils.get_config_log_html():
        logging.debug(f"Response HTML:\n{driver.page_source}")
    html_element = driver.find_element(By.TAG_NAME, "html")
    page_title = driver.title

    # find access denied titles
    for title in ACCESS_DENIED_TITLES:
        if page_title.startswith(title):
            raise Exception('Cloudflare has blocked this request. '
                            'Probably your IP is banned for this site, check in your web browser.')
    # find access denied selectors
    for selector in ACCESS_DENIED_SELECTORS:
        found_elements = driver.find_elements(By.CSS_SELECTOR, selector)
        if len(found_elements) > 0:
            raise Exception('Cloudflare has blocked this request. '
                            'Probably your IP is banned for this site, check in your web browser.')

    # find challenge by title
    challenge_found = False
    for title in CHALLENGE_TITLES:
        if title.lower() == page_title.lower():
            challenge_found = True
            logging.info("Challenge detected. Title found: " + page_title)
            break
    if not challenge_found:
        # find challenge by selectors
        for selector in CHALLENGE_SELECTORS:
            found_elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if len(found_elements) > 0:
                challenge_found = True
                logging.info("Challenge detected. Selector found: " + selector)
                break

    attempt = 0
    if challenge_found:
        while True:
            try:
                attempt = attempt + 1
                # wait until the title changes
                for title in CHALLENGE_TITLES:
                    logging.debug("Waiting for title (attempt " + str(attempt) + "): " + title)
                    WebDriverWait(driver, SHORT_TIMEOUT).until_not(title_is(title))

                # then wait until all the selectors disappear
                for selector in CHALLENGE_SELECTORS:
                    logging.debug("Waiting for selector (attempt " + str(attempt) + "): " + selector)
                    WebDriverWait(driver, SHORT_TIMEOUT).until_not(
                        presence_of_element_located((By.CSS_SELECTOR, selector)))

                # all elements not found
                break

            except TimeoutException:
                logging.debug("Timeout waiting for selector")

                click_verify(driver)

                # update the html (cloudflare reloads the page every 5 s)
                html_element = driver.find_element(By.TAG_NAME, "html")

        # waits until cloudflare redirection ends
        logging.debug("Waiting for redirect")
        # noinspection PyBroadException
        try:
            WebDriverWait(driver, SHORT_TIMEOUT).until(staleness_of(html_element))
        except Exception:
            logging.debug("Timeout waiting for redirect")

        logging.info("Challenge solved!")
        res.message = "Challenge solved!"
    else:
        logging.info("Challenge not detected!")
        res.message = "Challenge not detected!"

    challenge_res = ChallengeResolutionResultT({})
    challenge_res.url = driver.current_url
    challenge_res.status = 200  # todo: fix, selenium not provides this info
    challenge_res.cookies = driver.get_cookies()
    challenge_res.userAgent = utils.get_user_agent(driver)
    challenge_res.turnstile_token = turnstile_token

    if not req.returnOnlyCookies:
        challenge_res.headers = {}  # todo: fix, selenium not provides this info

        if req.waitInSeconds and req.waitInSeconds > 0:
            logging.info("Waiting " + str(req.waitInSeconds) + " seconds before returning the response...")
            time.sleep(req.waitInSeconds)

        challenge_res.response = driver.page_source

    if req.returnScreenshot:
        challenge_res.screenshot = driver.get_screenshot_as_base64()

    res.result = challenge_res
    return res


def _post_request(req: V1RequestBase, driver: WebDriver):
    post_form = f'<form id="hackForm" action="{req.url}" method="POST">'
    query_string = req.postData if req.postData and req.postData[0] != '?' else req.postData[1:] if req.postData else ''
    pairs = query_string.split('&')
    for pair in pairs:
        parts = pair.split('=', 1)
        # noinspection PyBroadException
        try:
            name = unquote(parts[0])
        except Exception:
            name = parts[0]
        if name == 'submit':
            continue
        # noinspection PyBroadException
        try:
            value = unquote(parts[1]) if len(parts) > 1 else ''
        except Exception:
            value = parts[1] if len(parts) > 1 else ''
        # Protection of " character, for syntax
        value=value.replace('"','&quot;')
        post_form += f'<input type="text" name="{escape(quote(name))}" value="{escape(quote(value))}"><br>'
    post_form += '</form>'
    html_content = f"""
        <!DOCTYPE html>
        <html>
        <body>
            {post_form}
            <script>document.getElementById('hackForm').submit();</script>
        </body>
        </html>"""
    driver.get("data:text/html;charset=utf-8,{html_content}".format(html_content=html_content))

