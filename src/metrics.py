import logging

from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time

REQUEST_COUNTER = Counter(
    name='flaresolverr_request',
    documentation='Total requests with result',
    labelnames=['domain', 'result', 'cmd']
)
REQUEST_DURATION = Histogram(
    name='flaresolverr_request_duration',
    documentation='Request duration in seconds',
    labelnames=['domain', 'cmd'],
    buckets=[10, 30, 60, 120, 300, 600]
)

WEBSOCKET_LOGGER_SESSION_TOTAL = Counter(
    name='flaresolverr_websocket_logger_session_total',
    documentation='Total successfully established websocket logger sessions',
    labelnames=['url']
)

WEBSOCKET_BYTES_RECEIVED_TOTAL = Counter(
    name='flaresolverr_websocket_bytes_received_total',
    documentation='Total bytes received from websocket connections',
    labelnames=['url']
)

WS_LISTENERS_ACTIVE = Gauge(
    name='flaresolverr_ws_listeners_active',
    documentation='Current number of active websocket listeners'
)

WS_LISTENERS_RUNNING = Gauge(
    name='flaresolverr_ws_listeners_running',
    documentation='Current number of RUNNING websocket listeners (excludes starting and unhealthy)'
)

WS_LISTENERS_STATUS = Gauge(
    name='flaresolverr_ws_listeners_status',
    documentation='Number of websocket listeners per status (starting, running, unhealthy)',
    labelnames=['status']
)

WS_LISTENERS_TOTAL = Counter(
    name='flaresolverr_ws_listeners_total',
    documentation='Total websocket listener lifecycle events',
    labelnames=['event']
)

WS_RECONNECT_TOTAL = Counter(
    name='flaresolverr_ws_reconnect_total',
    documentation='Total websocket listener reconnect attempts by result',
    labelnames=['url', 'result']
)

WS_MESSAGES_TOTAL = Counter(
    name='flaresolverr_ws_messages_total',
    documentation='Total websocket frames captured by listeners',
    labelnames=['url', 'type']
)

WS_SESSION_DURATION = Histogram(
    name='flaresolverr_ws_session_duration_seconds',
    documentation='Duration of websocket listener sessions in seconds',
    labelnames=['url'],
    buckets=[60, 300, 600, 900, 1800, 3600, 7200, 14400]
)

WS_LISTENER_ACTIVE = Gauge(
    name='flaresolverr_ws_listener_active',
    documentation='1 if the URL has at least one running/starting listener, 0 otherwise',
    labelnames=['url']
)

WS_LISTENER_UPTIME = Gauge(
    name='flaresolverr_ws_listener_uptime_seconds',
    documentation='Current active session duration in seconds for the URL primary listener',
    labelnames=['url']
)

WS_LISTENER_TOTAL_ACTIVE = Counter(
    name='flaresolverr_ws_listener_total_active_seconds',
    documentation='Cumulative active listening time in seconds per URL',
    labelnames=['url']
)

WS_LISTENER_LAST_SEEN = Gauge(
    name='flaresolverr_ws_listener_last_seen_timestamp',
    documentation='Unix timestamp of the last successful heartbeat for the URL listener',
    labelnames=['url']
)


PROCESS_THREADS_ACTIVE = Gauge(
    name='flaresolverr_process_threads_active',
    documentation='Current number of active threads in the FlareSolverr process'
)

THREAD_POOL_WORKERS = Gauge(
    name='flaresolverr_thread_pool_workers',
    documentation='Threads belonging to ThreadPoolExecutor pools'
)

GC_EVENT_LOOPS = Gauge(
    name='flaresolverr_gc_event_loops',
    documentation='Live asyncio event loop objects (gc census)'
)

GC_CHROME_DRIVERS = Gauge(
    name='flaresolverr_gc_chrome_drivers',
    documentation='Live undetected_chromedriver Chrome instances (gc census)')

PROCESS_RSS_BYTES = Gauge(
    name='flaresolverr_process_rss_bytes',
    documentation='Resident memory of the FlareSolverr process (from /proc/self/statm; 0 when unavailable)')


def serve(port):
    start_http_server(port=port)
    while True:
        time.sleep(600)


def start_metrics_http_server(prometheus_port: int):
    logging.info(f"Serving Prometheus exporter on http://0.0.0.0:{prometheus_port}/metrics")
    from threading import Thread
    Thread(
        target=serve,
        kwargs=dict(port=prometheus_port),
        daemon=True,
    ).start()
