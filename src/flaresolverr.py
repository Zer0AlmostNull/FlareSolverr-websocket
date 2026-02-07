import json
import logging
import os
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from tenacity import retry, wait_exponential, stop_after_delay, retry_if_exception_type

import certifi
from bottle import run, response, Bottle, request, ServerAdapter

from bottle_plugins.error_plugin import error_plugin
from bottle_plugins.logger_plugin import logger_plugin
from bottle_plugins import prometheus_plugin
from metrics import WEBSOCKET_LOGGER_SESSION_TOTAL, WEBSOCKET_BYTES_RECEIVED_TOTAL
from dtos import V1RequestBase, HealthResponse, STATUS_OK # Added HealthResponse, STATUS_OK
import flaresolverr_service
import utils

env_proxy_url = os.environ.get('PROXY_URL', None)
env_proxy_username = os.environ.get('PROXY_USERNAME', None)
env_proxy_password = os.environ.get('PROXY_PASSWORD', None)

# Global WebDriver instance for the single persistent websocket logger session
TARGET_URL = os.environ.get("TARGET_URL")
_websocket_logger_driver = None
_websocket_logger_session_id = None
WEBSOCKET_MESSAGES: deque[flaresolverr_service.WebsocketMessage] = deque(maxlen=500)
SESSION_HEALTH_CHECK_INTERVAL = int(os.environ.get("SESSION_HEALTH_CHECK_INTERVAL", 60)) # seconds
WEBSOCKET_LOGGER_SESSION_ID = "websocket_logger_session"


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
    Returns all collected websocket messages for a given session and clears the queue.
    """
    session_id = request.query.get('session')
    logging.info("Received request for /websocket_messages")

    # If a session id is provided, return messages from that session's queue.
    # If no session id is provided, return messages from the global queue and clear it.
    messages = []
    if session_id:
        try:
            session, _ = flaresolverr_service.SESSIONS_STORAGE.get(session_id)
            messages = [msg.__dict__ for msg in list(session.websocket_messages)]
            session.websocket_messages.clear()
        except Exception:
            response.status = 404
            return json.dumps({"error": f"Session with ID '{session_id}' not found"})
    else:
        messages = [msg.__dict__ for msg in list(WEBSOCKET_MESSAGES)]
        WEBSOCKET_MESSAGES.clear()

    response.content_type = 'application/json'
    return json.dumps(messages)

def _websocket_message_handler(event):
    params = event['params']
    frame_type = event['method'].split('.')[-1]  # "webSocketFrameReceived" or "webSocketFrameSent"

    url = params.get('url', _websocket_logger_driver.current_url if _websocket_logger_driver else "")
    payload = params['response']['payloadData'] if 'response' in params and 'payloadData' in params['response'] else \
              params.get('payloadData', '')

    websocket_msg = flaresolverr_service.WebsocketMessage(
        timestamp=time.time(),
        type=frame_type,
        url=url,
        payload=payload
    )
    WEBSOCKET_MESSAGES.append(websocket_msg)
    
    payload_bytes = len(payload.encode('utf-8'))
    logging.info(f"Websocket message from {url} {frame_type}: {payload_bytes} bytes")
    
    # Track bytes received from websocket
    if frame_type == "webSocketFrameReceived":
        WEBSOCKET_BYTES_RECEIVED_TOTAL.labels(url=url).inc(payload_bytes)


def _quit_websocket_logger_driver():
    global _websocket_logger_driver, _websocket_logger_session_id
    if _websocket_logger_driver:
        logging.info("Quitting existing WebSocket Logger WebDriver session...")
        try:
            _websocket_logger_driver.execute_cdp_cmd("Network.disable", {})
            _websocket_logger_driver.remove_cdp_listener("Network.webSocketFrameReceived", _websocket_message_handler)
            _websocket_logger_driver.remove_cdp_listener("Network.webSocketFrameSent", _websocket_message_handler)
            logging.debug("CDP websocket frame listeners removed and Network domain disabled for WebSocket Logger.")
            _websocket_logger_driver.quit()
        except Exception as e:
            logging.warning(f"Error while quitting WebSocket Logger driver: {e}")
        _websocket_logger_driver = None
    if _websocket_logger_session_id:
        try:
            if _websocket_logger_session_id:
                flaresolverr_service.SESSIONS_STORAGE.destroy(_websocket_logger_session_id)
        except Exception as e:
            logging.warning(f"Error while destroying WebSocket Logger session '{_websocket_logger_session_id}': {e}")
        _websocket_logger_session_id = None

@retry(wait=wait_exponential(multiplier=1, min=2, max=60), stop=stop_after_delay(3600), reraise=True)
def initialize_websocket_logger_session():
    global _websocket_logger_driver, _websocket_logger_session_id
    logging.info(f"Initializing single persistent session for URL: {TARGET_URL}")

    _quit_websocket_logger_driver()

    try:
        session, fresh = flaresolverr_service.SESSIONS_STORAGE.create(session_id=WEBSOCKET_LOGGER_SESSION_ID, proxy=None)
        _websocket_logger_session_id = session.session_id
        _websocket_logger_driver = session.driver
        logging.info(f"Persistent session '{_websocket_logger_session_id}' created successfully. Fresh: {fresh}")

        _websocket_logger_driver.execute_cdp_cmd("Network.enable", {})

        def _network_event_handler(event):
            method = event['method']
            params = event.get('params', {})
            request_id = params.get('requestId')
            url = params.get('request', {}).get('url') or params.get('response', {}).get('url') or params.get('redirectResponse', {}).get('url')

            log_message = f"CDP Network (WebSocket Logger): {method}"
            if request_id:
                log_message += f" (id: {request_id})"
            if url:
                log_message += f" (URL: {url})"

            logging.debug(log_message)


        _websocket_logger_driver.add_cdp_listener("Network.requestWillBeSent", _network_event_handler)
        _websocket_logger_driver.add_cdp_listener("Network.responseReceived", _network_event_handler)
        _websocket_logger_driver.add_cdp_listener("Network.loadingFinished", _network_event_handler)
        _websocket_logger_driver.add_cdp_listener("Network.loadingFailed", _network_event_handler)
        logging.debug("CDP general network event listeners added for WebSocket Logger.")

        _websocket_logger_driver.add_cdp_listener("Network.webSocketFrameReceived", _websocket_message_handler)
        _websocket_logger_driver.add_cdp_listener("Network.webSocketFrameSent", _websocket_message_handler)
        logging.info("CDP websocket frame listeners added for WebSocket Logger.")

        logging.info(f"Navigating persistent session to {TARGET_URL}")
        _websocket_logger_driver.get(TARGET_URL)
        logging.info(f"Navigation of persistent session to {TARGET_URL} complete.")
        
        # Track successful websocket logger session
        WEBSOCKET_LOGGER_SESSION_TOTAL.labels(url=TARGET_URL).inc()

    except Exception as e:
        logging.error(f"Failed to initialize persistent session or navigate to URL: {e}", exc_info=True)
        raise

def websocket_logger_health_checker_thread():
    global _websocket_logger_driver
    while True:
        time.sleep(SESSION_HEALTH_CHECK_INTERVAL)
        if _websocket_logger_driver:
            try:
                _ = _websocket_logger_driver.current_url
                logging.debug("WebSocket Logger WebDriver session is healthy.")
            except Exception as e:
                logging.error(f"WebSocket Logger WebDriver session became unhealthy: {e}. Attempting to re-initialize session.", exc_info=True)
                try:
                    initialize_websocket_logger_session()
                    logging.info("WebSocket Logger WebDriver session re-initialized successfully.")
                except Exception as re_e:
                    logging.critical(f"Failed to re-initialize WebSocket Logger WebDriver session after multiple retries: {re_e}", exc_info=True)
        else:
            logging.debug("WebSocket Logger WebDriver not initialized, waiting for it...")
            try:
                initialize_websocket_logger_session()
                logging.info("WebSocket Logger WebDriver session initialized by health checker.")
            except Exception as re_e:
                logging.critical(f"Failed to initialize WebSocket Logger WebDriver session by health checker: {re_e}", exc_info=True)

@app.get('/websocket_logger/messages')
def get_websocket_logger_messages():
    """
    Returns all collected websocket messages from the persistent logger session and clears the queue.
    """
    logging.info("Received request for /websocket_logger/messages")
    messages = [msg.__dict__ for msg in list(WEBSOCKET_MESSAGES)]
    WEBSOCKET_MESSAGES.clear()
    response.content_type = 'application/json'
    return json.dumps(messages)



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
    
    # Initialize and start persistent websocket logger session if TARGET_URL is provided
    if TARGET_URL:
        logging.info("TARGET_URL is set. Initializing persistent WebSocket Logger session...")
        try:
            initialize_websocket_logger_session()
        except Exception as e:
            logging.critical(f"Persistent WebSocket Logger session failed to initialize after multiple retries: {e}")
            sys.exit(1)

        health_thread = threading.Thread(target=websocket_logger_health_checker_thread, daemon=True)
        health_thread.start()
        logging.info("WebSocket Logger health checker thread started.")
    else:
        logging.info("TARGET_URL is not set. Persistent WebSocket Logger session will not be started.")

    run(app, host=server_host, port=server_port, quiet=True, server=WaitressServerPoll)
