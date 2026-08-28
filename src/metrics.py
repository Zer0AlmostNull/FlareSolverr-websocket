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


UNQUIT_CHROME_DRIVERS = Gauge(
    name='flaresolverr_unquit_chrome_drivers',
    documentation='WeakSet-tracked Chrome instances never quit(); should stay flat near listener count')


# New tab/lifecycle metrics
WS_TABS_ACTIVE = Gauge(
    name='flaresolverr_ws_tabs_active',
    documentation='Current number of active tabs in the single-Chrome manager'
)
WS_TABS_RUNNING = Gauge(
    name='flaresolverr_ws_tabs_running',
    documentation='Current number of RUNNING tabs in the single-Chrome manager'
)
WS_TAB_HANDOFF_TOTAL = Counter(
    name='flaresolverr_ws_tab_handoff_total',
    documentation='Total tab handoffs by result',
    labelnames=['url', 'result']
)
WS_TAB_RESTART_TOTAL = Counter(
    name='flaresolverr_ws_tab_restart_total',
    documentation='Total single-Chrome restarts by reason',
    labelnames=['reason']
)

# Additional observability metrics from review
WS_TAB_AGE = Gauge(
    name='flaresolverr_ws_tab_age_seconds',
    documentation='Age of tab in seconds',
    labelnames=['url', 'tab_id']
)
WS_FRAME_BUFFER_UTILIZATION = Gauge(
    name='flaresolverr_ws_frame_buffer_utilization',
    documentation='Frame buffer utilization (0-1)',
    labelnames=['url']
)
WS_HANDOFF_DURATION = Histogram(
    name='flaresolverr_ws_handoff_duration_seconds',
    documentation='Handoff duration in seconds',
    labelnames=['url'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
)
WS_LOOP_THREAD_ALIVE = Gauge(
    name='flaresolverr_ws_loop_thread_alive',
    documentation='1 if ChromeManager event loop thread is alive'
)

# Phase 2 & Phase 3 Resilient Capture Engine Metrics
WS_DATA_FRAME_RATE = Gauge(
    name='flaresolverr_ws_data_frame_rate',
    documentation='Incoming data frames per second',
    labelnames=['url']
)
WS_CONTROL_FRAME_RATE = Gauge(
    name='flaresolverr_ws_control_frame_rate',
    documentation='Incoming control/ping frames per second',
    labelnames=['url']
)
WS_STALL_ESCALATIONS = Counter(
    name='flaresolverr_ws_stall_escalations_total',
    documentation='Stall escalation events',
    labelnames=['url', 'tier']
)
WS_STANDBY_BROWSER_RECYCLES = Counter(
    name='flaresolverr_ws_standby_recycles_total',
    documentation='Standby browser recycling events',
    labelnames=['reason']
)
WS_STANDBY_HANDOFF_DURATION = Histogram(
    name='flaresolverr_ws_standby_handoff_duration_seconds',
    documentation='Duration of standby browser handoff'
)


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
