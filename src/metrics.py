import logging

from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time

REQUEST_COUNTER = Counter(
    name='flaresolverr_request',
    documentation='Total requests with result',
    labelnames=['domain', 'result']
)
REQUEST_DURATION = Histogram(
    name='flaresolverr_request_duration',
    documentation='Request duration in seconds',
    labelnames=['domain'],
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
