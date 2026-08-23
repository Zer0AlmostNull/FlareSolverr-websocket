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
        self.manager = flaresolverr.ws_listener_manager
        self.manager.listeners.clear()
        self.manager._url_index.clear()
        self._create_patch = patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                                          return_value=(_make_mock_session(), True))
        self._create_patch.start()

    def tearDown(self):
        self._create_patch.stop()
        self.manager.listeners.clear()
        self.manager._url_index.clear()

    def test_messages_without_listener_id_returns_400(self):
        res = self.app.get('/websocket_messages', status=400)
        self.assertIn("url", res.json["error"])

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

    def test_message_handler_falls_back_to_target_url(self):
        session = SimpleNamespace(
            websocket_messages=deque(),
            url_cache_time=9999999999,
            last_known_url="",
            target_url="https://listener.example",
            driver=SimpleNamespace(current_url=""),
        )
        flaresolverr_service._websocket_message_handler(session, {
            "method": "Network.webSocketFrameReceived",
            "params": {"response": {"payloadData": "hi"}},  # no url in params
        }, track_metrics=True)
        self.assertEqual(session.websocket_messages[0].url, "https://listener.example")

    def test_config_getters_defaults(self):
        with patch.dict('os.environ', {}, clear=True):
            self.assertEqual(utils.get_config_max_ws_listeners(), 5)
            self.assertEqual(utils.get_config_ws_listener_default_ttl(), 30)
            self.assertEqual(utils.get_config_ws_listener_default_max_msgs(), 500)

    def test_ws_listener_max_lifetime_default(self):
        with patch.dict('os.environ', {}, clear=True):
            self.assertEqual(utils.get_config_ws_listener_max_lifetime(), 180)
        with patch.dict('os.environ', {'WS_LISTENER_MAX_LIFETIME_MINUTES': '360'}):
            self.assertEqual(utils.get_config_ws_listener_max_lifetime(), 360)

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

    def test_websocket_messages_creates_listener_for_url(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            res = self.app.get('/websocket_messages?url=https://x.io')
        self.assertEqual(res.status_int, 200)
        self.assertIn("status", res.json)
        self.assertIn(res.json["status"], ("starting", "running"))
        self.assertNotIn("listener_id", res.json)
        self.assertNotIn("url", res.json)
        self.assertEqual(res.json["messages"], [])

    def test_websocket_messages_url_idempotent(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            self.app.get('/websocket_messages?url=https://x.io')
            self.app.get('/websocket_messages?url=https://x.io')
        self.assertEqual(len(flaresolverr.ws_listener_manager.listeners), 1)

    def test_websocket_messages_url_returns_status_and_messages(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            self.app.get('/websocket_messages?url=https://x.io')
        flaresolverr_service.SESSIONS_STORAGE.sessions[session.session_id] = session
        for _ in range(50):
            if flaresolverr.ws_listener_manager.listeners and \
               next(iter(flaresolverr.ws_listener_manager.listeners.values())).status == "running":
                break
            time.sleep(0.01)
        session.websocket_messages.append(
            flaresolverr_service.WebsocketMessage(1.0, "received", "wss://x.io", "data"))
        res = self.app.get('/websocket_messages?url=https://x.io')
        self.assertEqual(res.json["status"], "running")
        self.assertEqual(res.json["messages"][0]["payload"], "data")
        self.assertEqual(len(session.websocket_messages), 0)

    def test_websocket_messages_url_drains_and_clears(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            self.app.get('/websocket_messages?url=https://x.io')
        flaresolverr_service.SESSIONS_STORAGE.sessions[session.session_id] = session
        for _ in range(50):
            if flaresolverr.ws_listener_manager.listeners and \
               next(iter(flaresolverr.ws_listener_manager.listeners.values())).status == "running":
                break
            time.sleep(0.01)
        session.websocket_messages.append(
            flaresolverr_service.WebsocketMessage(1.0, "received", "wss://x.io", "a"))
        session.websocket_messages.append(
            flaresolverr_service.WebsocketMessage(2.0, "received", "wss://x.io", "b"))
        r1 = self.app.get('/websocket_messages?url=https://x.io')
        self.assertEqual([m["payload"] for m in r1.json["messages"]], ["a", "b"])
        r2 = self.app.get('/websocket_messages?url=https://x.io')
        self.assertEqual(r2.json["messages"], [])


    def test_session_create_accepts_target_url(self):
        from sessions import SessionsStorage
        storage = SessionsStorage()
        fake_driver = SimpleNamespace(quit=lambda: None)
        with patch("utils.get_webdriver", return_value=fake_driver):
            session, fresh = storage.create(session_id="t1", target_url="https://example.io")
        self.assertTrue(fresh)
        self.assertEqual(session.target_url, "https://example.io")
        # default remains "" for regular sessions
        with patch("utils.get_webdriver", return_value=fake_driver):
            session2, _ = storage.create(session_id="t2")
        self.assertEqual(session2.target_url, "")


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

    def test_ensure_listener_for_url_raises_at_max(self):
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=[(_make_mock_session("s1"), True),
                                       (_make_mock_session("s2"), True)]):
            self.manager.ensure_listener_for_url("https://a.io")
            self.manager.ensure_listener_for_url("https://b.io")
        with self.assertRaises(flaresolverr_service.MaxListenersReachedError):
            self.manager.ensure_listener_for_url("https://c.io")

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

    def test_get_listener_renews_exported_last_seen_gauge(self):
        from metrics import WS_LISTENER_LAST_SEEN
        url = 'https://heartbeat.io'
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(_make_mock_session(), True)):
            listener = self.manager.create_listener(url)
        before = WS_LISTENER_LAST_SEEN.labels(url=url)._value.get()
        time.sleep(1.1)  # ensure the epoch-second value advances
        self.manager.get_listener(listener.listener_id)
        after = WS_LISTENER_LAST_SEEN.labels(url=url)._value.get()
        self.assertGreater(after, before)
        self.assertAlmostEqual(after, time.time(), delta=5)

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
        # after destroy the per-url series is removed entirely
        samples = [s for s in WS_LISTENER_ACTIVE.collect()[0].samples
                   if s.labels.get('url') == url]
        self.assertEqual(samples, [])

    def test_destroy_removes_last_seen_series_instead_of_zeroing(self):
        from metrics import WS_LISTENER_ACTIVE, WS_LISTENER_LAST_SEEN, WS_LISTENER_UPTIME
        url = 'https://zero-test.io'
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(_make_mock_session(), True)):
            listener = self.manager.create_listener(url)
        # seed the series so we can observe removal
        WS_LISTENER_LAST_SEEN.labels(url=url).set(1700000000.0)
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy", return_value=True):
            self.manager.destroy_listener(listener.listener_id)
        samples = lambda g: [s for s in g.collect()[0].samples if s.labels.get('url') == url]
        self.assertEqual(samples(WS_LISTENER_LAST_SEEN), [])
        self.assertEqual(samples(WS_LISTENER_ACTIVE), [])
        self.assertEqual(samples(WS_LISTENER_UPTIME), [])

    def test_destroy_keeps_series_when_other_listener_serves_same_url(self):
        from metrics import WS_LISTENER_LAST_SEEN
        url = 'https://shared-url.io'
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=[(_make_mock_session("s1"), True),
                                       (_make_mock_session("s2"), True)]):
            l1 = self.manager.create_listener(url)
            l2 = self.manager.create_listener(url)  # second listener, same URL
            # force distinct ids in _url_index: create_listener overwrites the index,
            # simulate both being tracked
            self.manager.listeners[l1.listener_id] = l1
        try:
            with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy", return_value=True):
                self.manager.destroy_listener(l2.listener_id)
            value = WS_LISTENER_LAST_SEEN.labels(url=url)._value.get()
            self.assertGreater(value, 0)
        finally:
            with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy", return_value=True):
                self.manager.destroy_listener(l1.listener_id)

    def test_start_session_failure_clears_per_url_metrics(self):
        from metrics import WS_LISTENER_ACTIVE, WS_LISTENER_LAST_SEEN, WS_LISTENER_UPTIME
        url = 'https://fail-start.io'
        # seed the series for a url that has no live listener
        WS_LISTENER_LAST_SEEN.labels(url=url).set(1700000000.0)
        WS_LISTENER_ACTIVE.labels(url=url).set(1)
        WS_LISTENER_UPTIME.labels(url=url).set(5.0)
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=Exception("browser dead")):
            with patch.object(utils, "kill_orphaned_chrome", lambda *a: None):
                with self.assertRaises(Exception):
                    self.manager.create_listener(url)
        samples = lambda g: [s for s in g.collect()[0].samples if s.labels.get('url') == url]
        self.assertEqual(samples(WS_LISTENER_LAST_SEEN), [])
        self.assertEqual(samples(WS_LISTENER_ACTIVE), [])
        self.assertEqual(samples(WS_LISTENER_UPTIME), [])


class TestWebSocketListenerEndpoints(unittest.TestCase):
    def setUp(self):
        self.app = TestApp(flaresolverr.app)
        self.manager = flaresolverr.ws_listener_manager
        self._orig_create = flaresolverr_service.SESSIONS_STORAGE.create
        self._orig_destroy = flaresolverr_service.SESSIONS_STORAGE.destroy
        self.manager.listeners.clear()
        self.manager._url_index.clear()
        self._create_patch = patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                                          return_value=(_make_mock_session(), True))
        self._create_patch.start()

    def tearDown(self):
        self._create_patch.stop()
        flaresolverr_service.SESSIONS_STORAGE.create = self._orig_create
        flaresolverr_service.SESSIONS_STORAGE.destroy = self._orig_destroy
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()
        self.manager.listeners.clear()
        self.manager._url_index.clear()

    def test_create_listener_via_get(self):
        session = _make_mock_session()
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          return_value=(session, True)):
            res = self.app.get('/websocket_messages?url=https://x.io')
        self.assertEqual(res.status_int, 200)
        self.assertIn(res.json["status"], ("starting", "running"))
        self.assertNotIn("listener_id", res.json)


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

    def test_ws_listeners_running_metric_excludes_starting(self):
        """Test that WS_LISTENERS_RUNNING only counts 'running' listeners, not 'starting'."""
        from flaresolverr_service import WebSocketListenerManager, WebSocketListener
        from metrics import WS_LISTENERS_RUNNING, WS_LISTENERS_ACTIVE
        from datetime import datetime
        
        manager = WebSocketListenerManager(max_listeners=5)
        
        # Manually add listeners with different statuses (bypass browser startup)
        listener1 = WebSocketListener(listener_id="1", url="http://example.com", status="starting")
        listener2 = WebSocketListener(listener_id="2", url="http://example2.com", status="running")
        listener3 = WebSocketListener(listener_id="3", url="http://example3.com", status="unhealthy")
        listener4 = WebSocketListener(listener_id="4", url="http://example4.com", status="running")
        
        manager.listeners = {
            "1": listener1, "2": listener2, "3": listener3, "4": listener4
        }
        manager._url_index = {
            "http://example.com": "1",
            "http://example2.com": "2",
            "http://example3.com": "3",
            "http://example4.com": "4"
        }
        
        manager._update_gauges()
        
        # WS_LISTENERS_ACTIVE should be 4 (all listeners)
        assert WS_LISTENERS_ACTIVE._value.get() == 4
        
        # WS_LISTENERS_RUNNING should be 2 (only "running" status)
        assert WS_LISTENERS_RUNNING._value.get() == 2


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
