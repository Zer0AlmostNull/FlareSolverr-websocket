import json
import logging
import os
import sys
import threading
import time

import certifi
from bottle import run, response, Bottle, request, ServerAdapter

from bottle_plugins.error_plugin import error_plugin
from bottle_plugins.logger_plugin import logger_plugin
from bottle_plugins import prometheus_plugin
from dtos import V1RequestBase, HealthResponse, STATUS_OK # Added HealthResponse, STATUS_OK
import flaresolverr_service
import utils

env_proxy_url = os.environ.get('PROXY_URL', None)
env_proxy_username = os.environ.get('PROXY_USERNAME', None)
env_proxy_password = os.environ.get('PROXY_PASSWORD', None)

SESSION_HEALTH_CHECK_INTERVAL = int(os.environ.get("SESSION_HEALTH_CHECK_INTERVAL", 60)) # seconds

ws_listener_manager = flaresolverr_service.WebSocketListenerManager(
    max_listeners=utils.get_config_max_ws_listeners())


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
    try:
        payload = ws_listener_manager.ensure_and_fetch(url)
    except flaresolverr_service.MaxListenersReachedError:
        response.status = 429
        response.content_type = 'application/json'
        return json.dumps({"error": "Max listeners reached"})
    response.content_type = 'application/json'
    return json.dumps(payload)

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
