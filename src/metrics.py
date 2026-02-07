import logging

from prometheus_client import Counter, Histogram, start_http_server
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
    buckets=[0, 10, 25, 50]
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
