import os
import time
import unittest
from collections import deque
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from webtest import TestApp

import flaresolverr
import flaresolverr_service
import utils
import metrics



class TestWebsocketCapture(unittest.TestCase):
    def setUp(self):
        self.app = TestApp(flaresolverr.app)
        flaresolverr.ws_listener_manager.listeners.clear()

    def tearDown(self):
        flaresolverr.ws_listener_manager.listeners.clear()

    def test_messages_without_listener_id_returns_400(self):
        res = self.app.get('/websocket_messages', status=400)
        self.assertIn("listener_id", res.json["error"])

    def test_session_endpoint_returns_and_clears_session_queue(self):
        session = SimpleNamespace(websocket_messages=deque([
            flaresolverr_service.WebsocketMessage(2.0, "sent", "wss://example.test", "out")
        ]))
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "get", return_value=(session, False)):
            response = self.app.get('/websocket_messages?session=session-1')

        self.assertEqual(response.json[0]["payload"], "out")
        self.assertEqual(len(session.websocket_messages), 0)

    def test_capture_extracts_frame_type_url_and_payload(self):
        session = SimpleNamespace(
            websocket_messages=deque(),
            url_cache_time=0,
            last_known_url="",
            driver=SimpleNamespace(current_url="https://page.example"),
        )
        flaresolverr_service._websocket_message_handler(session, {
            "method": "Network.webSocketFrameReceived",
            "params": {"url": "wss://socket.example", "response": {"payloadData": "hello"}},
        })

        message = session.websocket_messages[0]
        self.assertEqual((message.type, message.url, message.payload),
                         ("webSocketFrameReceived", "wss://socket.example", "hello"))

    def test_config_getters_defaults(self):
        with patch.dict('os.environ', {}, clear=True):
            self.assertEqual(utils.get_config_max_ws_listeners(), 5)
            self.assertEqual(utils.get_config_ws_listener_default_ttl(), 30)
            self.assertEqual(utils.get_config_ws_listener_default_max_msgs(), 500)

    def test_websocket_listener_dataclass_defaults(self):
        now = datetime.now()
        listener = flaresolverr_service.WebSocketListener(
            listener_id="id", session_id="sid", url="https://x.io",
            created_at=now, last_heartbeat=now,
        )
        self.assertEqual(listener.ttl_minutes, 30)
        self.assertEqual(listener.max_messages, 500)
        self.assertEqual(listener.status, "starting")
        self.assertEqual(listener.error_message, "")

    def test_listener_messages_listener_not_found_returns_404(self):
        res = self.app.get('/v1/ws/listeners/nonexistent/messages', status=404)
        self.assertIn("not found", res.json["error"])

    def test_listener_messages_returns_and_clears(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            listener = flaresolverr.ws_listener_manager.create_listener("https://x.io")
        flaresolverr_service.SESSIONS_STORAGE.sessions[session.session_id] = session
        session.websocket_messages.append(
            flaresolverr_service.WebsocketMessage(1.0, "received", "wss://x.io", "data")
        )
        res = self.app.get(f'/v1/ws/listeners/{listener.listener_id}/messages')
        self.assertEqual(res.json[0]["type"], "received")
        self.assertEqual(len(session.websocket_messages), 0)

    def test_messages_with_listener_id_returns_and_clears(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            listener = flaresolverr.ws_listener_manager.create_listener("https://x.io")
        flaresolverr_service.SESSIONS_STORAGE.sessions[session.session_id] = session
        session.websocket_messages.append(
            flaresolverr_service.WebsocketMessage(2.0, "sent", "wss://x.io", "out")
        )
        res = self.app.get(f'/websocket_messages?listener_id={listener.listener_id}')
        self.assertEqual(res.json[0]["payload"], "out")
        self.assertEqual(len(session.websocket_messages), 0)

    def test_messages_with_unknown_listener_id_returns_404(self):
        res = self.app.get('/websocket_messages?listener_id=nonexistent', status=404)
        self.assertIn("not found", res.json["error"])

    def test_listeners_create_endpoint(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            res = self.app.post_json('/v1/ws/listeners',
                                     {"url": "https://x.io"})
        self.assertEqual(res.status_int, 200)
        self.assertIn("listener_id", res.json)
        self.assertEqual(res.json["url"], "https://x.io")

    def test_listeners_list_endpoint(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            flaresolverr.ws_listener_manager.create_listener("https://x.io")
        res = self.app.get('/v1/ws/listeners')
        self.assertEqual(res.status_int, 200)
        self.assertEqual(len(res.json), 1)
        self.assertEqual(res.json[0]["url"], "https://x.io")


def _make_mock_session(session_id="s1", maxlen=500):
    driver = SimpleNamespace(
        execute_cdp_cmd=lambda *a, **k: None,
        add_cdp_listener=lambda *a, **k: None,
        remove_cdp_listener=lambda *a, **k: None,
        current_url="https://x.io",
        get=lambda url: None,
        quit=lambda: None,
        close=lambda: None,
    )
    return SimpleNamespace(
        session_id=session_id,
        driver=driver,
        websocket_messages=deque(maxlen=maxlen),
        url_cache_time=0,
        last_known_url="",
    )


class TestWebSocketManagerLifecycle(unittest.TestCase):
    def setUp(self):
        self.manager = flaresolverr_service.WebSocketListenerManager(max_listeners=2)

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def test_cleanup_stale_destroys_expired(self):
        session = _make_mock_session()
        flaresolverr_service.SESSIONS_STORAGE.sessions["s1"] = session
        try:
            with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                             return_value=(session, True)):
                listener = self.manager.create_listener("https://x.io")
            listener.last_heartbeat = datetime.now() - timedelta(
                minutes=listener.ttl_minutes + 1)
            with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                             return_value=True) as mock_destroy:
                self.manager.cleanup_stale()
            mock_destroy.assert_called_once_with("s1")
            self.assertEqual(len(self.manager.listeners), 0)
        finally:
            flaresolverr_service.SESSIONS_STORAGE.sessions.pop("s1", None)

    def test_cleanup_stale_destroys_when_session_missing(self):
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                         return_value=(_make_mock_session(), True)):
            listener = self.manager.create_listener("https://x.io")
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                         return_value=True) as mock_destroy:
            self.manager.cleanup_stale()
        mock_destroy.assert_called_once()
        self.assertEqual(len(self.manager.listeners), 0)

    def test_cleanup_stale_reconnects_unhealthy(self):
        s1 = _make_mock_session("s1")
        flaresolverr_service.SESSIONS_STORAGE.sessions["s1"] = s1
        try:
            with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                             return_value=(s1, True)):
                listener = self.manager.create_listener("https://x.io")
            def boom():
                raise Exception("driver dead")
            s1.driver.current_url = boom
            s2 = _make_mock_session("s1")
            flaresolverr_service.SESSIONS_STORAGE.sessions["s1"] = s2
            with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                             return_value=(s2, True)):
                with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                                 return_value=True):
                    self.manager.cleanup_stale()
            self.assertEqual(listener.status, "running")
            self.assertEqual(listener.error_message, "")
        finally:
            flaresolverr_service.SESSIONS_STORAGE.sessions.pop("s1", None)


class TestWebSocketListenerManager(unittest.TestCase):
    def setUp(self):
        self.manager = flaresolverr_service.WebSocketListenerManager(max_listeners=2)
        self._orig_create = flaresolverr_service.SESSIONS_STORAGE.create
        self._orig_destroy = flaresolverr_service.SESSIONS_STORAGE.destroy

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.create = self._orig_create
        flaresolverr_service.SESSIONS_STORAGE.destroy = self._orig_destroy
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def test_create_listener(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                         return_value=(session, True)):
            listener = self.manager.create_listener("https://x.io")
        self.assertIn(listener.listener_id, self.manager.listeners)
        self.assertEqual(listener.status, "running")
        self.assertEqual(listener.session_id, "s1")
        self.assertEqual(listener.url, "https://x.io")

    def test_create_listener_respects_max(self):
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                         side_effect=[(_make_mock_session("s1"), True),
                                      (_make_mock_session("s2"), True)]):
            self.manager.create_listener("https://a.io")
            self.manager.create_listener("https://b.io")
            with self.assertRaises(flaresolverr_service.MaxListenersReachedError):
                self.manager.create_listener("https://c.io")

    def test_create_requires_url(self):
        with self.assertRaises(ValueError):
            self.manager.create_listener("")

    def test_get_listener_renews_heartbeat(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                         return_value=(session, True)):
            listener = self.manager.create_listener("https://x.io")
        old = listener.last_heartbeat
        time.sleep(0.01)
        self.manager.get_listener(listener.listener_id)
        self.assertGreater(listener.last_heartbeat, old)

    def test_destroy_listener(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                         return_value=(session, True)):
            listener = self.manager.create_listener("https://x.io")
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                         return_value=True) as mock_destroy:
            self.assertTrue(self.manager.destroy_listener(listener.listener_id))
        mock_destroy.assert_called_once_with("s1")
        self.assertNotIn(listener.listener_id, self.manager.listeners)

    def test_destroy_missing_returns_false(self):
        self.assertFalse(self.manager.destroy_listener("nope"))

    def test_create_and_cleanup_emits_per_url_metrics(self):
        from metrics import WS_LISTENER_ACTIVE, WS_LISTENER_LAST_SEEN, WS_LISTENER_UPTIME
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        url = 'https://mevx.io/?chain=solana'
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(_make_mock_session(), True)):
            listener = mgr.create_listener(url)
        try:
            # active=1, last_seen set, uptime >= 0
            self.assertEqual(WS_LISTENER_ACTIVE.labels(url=url)._value.get(), 1)
            self.assertGreater(WS_LISTENER_LAST_SEEN.labels(url=url)._value.get(), 0)
            self.assertGreaterEqual(WS_LISTENER_UPTIME.labels(url=url)._value.get(), 0)
        finally:
            with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy", return_value=True):
                mgr.destroy_listener(listener.listener_id)
        # after destroy active goes to 0
        self.assertEqual(WS_LISTENER_ACTIVE.labels(url=url)._value.get(), 0)


class TestWebSocketListenerEndpoints(unittest.TestCase):
    def setUp(self):
        self.app = TestApp(flaresolverr.app)
        self.manager = flaresolverr.ws_listener_manager
        self._orig_create = flaresolverr_service.SESSIONS_STORAGE.create
        self._orig_destroy = flaresolverr_service.SESSIONS_STORAGE.destroy
        self.manager.listeners.clear()

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.create = self._orig_create
        flaresolverr_service.SESSIONS_STORAGE.destroy = self._orig_destroy
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()
        self.manager.listeners.clear()

    def test_create_listener(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            res = self.app.post_json('/v1/ws/listeners',
                                     {"url": "https://x.io"})
        self.assertEqual(res.status_int, 200)
        self.assertEqual(res.json["url"], "https://x.io")
        self.assertEqual(res.json["status"], "running")

    def test_list_listeners(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            self.manager.create_listener("https://x.io")
        res = self.app.get('/v1/ws/listeners')
        self.assertEqual(res.status_int, 200)
        self.assertEqual(len(res.json), 1)
        self.assertEqual(res.json[0]["url"], "https://x.io")
        self.assertIn("message_count", res.json[0])

    def test_get_listener(self):
        session = _make_mock_session()
        flaresolverr_service.SESSIONS_STORAGE.sessions["s1"] = session
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            listener = self.manager.create_listener("https://x.io")
        res = self.app.get(f'/v1/ws/listeners/{listener.listener_id}')
        self.assertEqual(res.status_int, 200)
        self.assertEqual(res.json["url"], "https://x.io")
        self.assertIn("ttl_minutes", res.json)
        self.assertIn("max_messages", res.json)
        self.assertIn("message_count", res.json)

    def test_get_listener_not_found(self):
        res = self.app.get('/v1/ws/listeners/nonexistent', status=404)
        self.assertIn("not found", res.json["error"])

    def test_delete_listener(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            listener = self.manager.create_listener("https://x.io")
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True):
            res = self.app.delete(f'/v1/ws/listeners/{listener.listener_id}')
        self.assertEqual(res.status_int, 200)
        self.assertTrue(res.json["success"])

    def test_delete_listener_not_found(self):
        res = self.app.delete('/v1/ws/listeners/nonexistent', status=404)
        self.assertIn("not found", res.json["error"])


class TestWebSocketMetrics(unittest.TestCase):
    def setUp(self):
        self.manager = flaresolverr_service.WebSocketListenerManager(max_listeners=2)
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def test_create_listener_counts_created_and_active(self):
        created_before = metrics.WS_LISTENERS_TOTAL.labels(event="created")._value.get()
        active_before = metrics.WS_LISTENERS_ACTIVE._value.get()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(_make_mock_session(), True)):
            self.manager.create_listener("https://x.io")
        self.assertEqual(
            metrics.WS_LISTENERS_TOTAL.labels(event="created")._value.get(),
            created_before + 1)
        self.assertEqual(metrics.WS_LISTENERS_ACTIVE._value.get(), active_before + 1)
        self.assertEqual(
            metrics.WS_LISTENERS_STATUS.labels(status="running")._value.get(), 1)

    def test_destroy_listener_counts_destroyed_and_decrements_active(self):
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(_make_mock_session(), True)):
            listener = self.manager.create_listener("https://x.io")
        destroyed_before = metrics.WS_LISTENERS_TOTAL.labels(event="destroyed")._value.get()
        active_before = metrics.WS_LISTENERS_ACTIVE._value.get()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy", return_value=True):
            self.manager.destroy_listener(listener.listener_id)
        self.assertEqual(
            metrics.WS_LISTENERS_TOTAL.labels(event="destroyed")._value.get(),
            destroyed_before + 1)
        self.assertEqual(metrics.WS_LISTENERS_ACTIVE._value.get(), active_before - 1)

    def test_cleanup_stale_ttl_expired_counts_event(self):
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(_make_mock_session(), True)):
            listener = self.manager.create_listener("https://x.io")
        listener.last_heartbeat = datetime.now() - timedelta(minutes=listener.ttl_minutes + 1)
        expired_before = metrics.WS_LISTENERS_TOTAL.labels(event="ttl_expired")._value.get()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy", return_value=True):
            self.manager.cleanup_stale()
        self.assertEqual(
            metrics.WS_LISTENERS_TOTAL.labels(event="ttl_expired")._value.get(),
            expired_before + 1)

    def test_websocket_message_handler_counts_only_when_tracked(self):
        session = _make_mock_session()
        session.url_cache_time = 0
        session.last_known_url = ""
        received_before = metrics.WS_MESSAGES_TOTAL.labels(
            url="wss://x.io", type="received")._value.get()
        flaresolverr_service._websocket_message_handler(session, {
            "method": "Network.webSocketFrameReceived",
            "params": {"url": "wss://x.io", "response": {"payloadData": "hi"}},
        }, track_metrics=True)
        self.assertEqual(
            metrics.WS_MESSAGES_TOTAL.labels(url="wss://x.io", type="received")._value.get(),
            received_before + 1)
        before2 = metrics.WS_MESSAGES_TOTAL.labels(
            url="wss://x.io", type="received")._value.get()
        flaresolverr_service._websocket_message_handler(session, {
            "method": "Network.webSocketFrameReceived",
            "params": {"url": "wss://x.io", "response": {"payloadData": "hi"}},
        }, track_metrics=False)
        self.assertEqual(
            metrics.WS_MESSAGES_TOTAL.labels(url="wss://x.io", type="received")._value.get(),
            before2)

    def test_max_listeners_counts_max_reached(self):
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=[(_make_mock_session("s1"), True),
                                       (_make_mock_session("s2"), True)]):
            self.manager.create_listener("https://a.io")
            self.manager.create_listener("https://b.io")
        max_before = metrics.WS_LISTENERS_TOTAL.labels(event="max_reached")._value.get()
        with self.assertRaises(flaresolverr_service.MaxListenersReachedError):
            self.manager.create_listener("https://c.io")
        self.assertEqual(
            metrics.WS_LISTENERS_TOTAL.labels(event="max_reached")._value.get(),
            max_before + 1)


class TestMetricsModule(unittest.TestCase):
    def test_new_metrics_registered_with_url_label(self):
        from metrics import (
            WS_LISTENER_ACTIVE, WS_LISTENER_UPTIME,
            WS_LISTENER_TOTAL_ACTIVE, WS_LISTENER_LAST_SEEN,
        )
        for m in (WS_LISTENER_ACTIVE, WS_LISTENER_UPTIME,
                  WS_LISTENER_TOTAL_ACTIVE, WS_LISTENER_LAST_SEEN):
            # prometheus_client metrics expose ._labelnames
            self.assertIn('url', m._labelnames)
        # smoke: setting with a url label works
        WS_LISTENER_ACTIVE.labels(url='https://mevx.io/?chain=solana').set(1)
        WS_LISTENER_UPTIME.labels(url='https://mevx.io/?chain=solana').set(123.0)
        WS_LISTENER_TOTAL_ACTIVE.labels(url='https://mevx.io/?chain=solana').inc(123.0)
        WS_LISTENER_LAST_SEEN.labels(url='https://mevx.io/?chain=solana').set(1700000000.0)

    def test_ws_metric_objects_defined(self):
        expected = {
            "WS_LISTENERS_ACTIVE": "flaresolverr_ws_listeners_active",
            "WS_LISTENERS_STATUS": "flaresolverr_ws_listeners_status",
            "WS_LISTENERS_TOTAL": "flaresolverr_ws_listeners_total",
            "WS_RECONNECT_TOTAL": "flaresolverr_ws_reconnect_total",
            "WS_MESSAGES_TOTAL": "flaresolverr_ws_messages_total",
            "WS_SESSION_DURATION": "flaresolverr_ws_session_duration_seconds",
        }
        from prometheus_client import REGISTRY
        registered = set(REGISTRY._names_to_collectors.keys())
        for var_name, metric_name in expected.items():
            self.assertTrue(hasattr(metrics, var_name), f"missing {var_name}")
            self.assertIn(metric_name, registered, f"metric {metric_name} not registered")


if __name__ == '__main__':
    unittest.main()
