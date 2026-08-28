import inspect
import json
import os
import threading
import time
import unittest
from collections import deque
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from webtest import TestApp

import flaresolverr
import flaresolverr_service
import utils
import metrics
import undetected_chromedriver as uc_init
from undetected_chromedriver import reactor


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
            self.assertEqual(utils.get_config_ws_listener_default_max_msgs(), 2000)

    def test_ws_listener_max_lifetime_default(self):
        with patch.dict('os.environ', {}, clear=True):
            self.assertEqual(utils.get_config_ws_listener_max_lifetime(), 180)
        with patch.dict('os.environ', {'WS_LISTENER_MAX_LIFETIME_MINUTES': '360'}):
            self.assertEqual(utils.get_config_ws_listener_max_lifetime(), 360)

    def test_ws_chrome_v8_heap_mb_default_and_env(self):
        with patch.dict('os.environ', {}, clear=True):
            self.assertEqual(utils.get_config_ws_chrome_v8_heap_mb(), 1024)
        with patch.dict('os.environ', {'WS_CHROME_V8_HEAP_MB': '512'}):
            self.assertEqual(utils.get_config_ws_chrome_v8_heap_mb(), 512)

    def test_websocket_listener_dataclass_defaults(self):
        now = datetime.now()
        listener = flaresolverr_service.WebSocketListener(
            listener_id="id", session_id="sid", url="https://x.io",
            created_at=now, last_heartbeat=now,
        )
        self.assertEqual(listener.ttl_minutes, 30)
        self.assertEqual(listener.max_messages, 2000)
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


class TestWebSocketMessagesFlag(unittest.TestCase):
    """Route /websocket_messages through TabManager when WS_TAB_MANAGER_ENABLED,
    else the legacy WebSocketListenerManager. _ensure_tab_manager() reads the
    getter at call time (utils.get_config_ws_tab_manager_enabled reads os.environ
    per call), so env patches take effect per test."""

    def setUp(self):
        self.app = TestApp(flaresolverr.app)
        flaresolverr.ws_listener_manager.listeners.clear()
        flaresolverr.ws_listener_manager._url_index.clear()

    def tearDown(self):
        flaresolverr.ws_listener_manager.listeners.clear()
        flaresolverr.ws_listener_manager._url_index.clear()
        # Reset the lazy singleton between tests so env patches take effect.
        flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'false'}, clear=False)
    def test_flag_off_uses_legacy_manager(self):
        # _ensure_tab_manager() returns None when flag is off -> legacy path.
        with patch.object(flaresolverr_service.WebSocketListenerManager,
                          'ensure_and_fetch',
                          return_value={"status": "running", "messages": []}):
            res = self.app.get('/websocket_messages?url=https://x.io')
        self.assertEqual(res.json["status"], "running")

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true'}, clear=False)
    def test_flag_on_uses_tab_manager(self):
        # Inject a stub service into the lazy singleton to prove routing WITHOUT
        # launching a real Chrome during the unit test.
        stub = Mock(spec=["ensure_and_fetch"])
        stub.ensure_and_fetch.return_value = {"status": "running", "messages": []}
        flaresolverr._tab_mgr["service"] = stub
        res = self.app.get('/websocket_messages?url=https://x.io')
        self.assertEqual(res.json["status"], "running")
        stub.ensure_and_fetch.assert_called_once_with("https://x.io")

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true'}, clear=False)
    def test_tab_manager_max_tabs_returns_429(self):
        # ensure_and_fetch raises MaxTabsReachedError -> 429 response.
        from chrome_manager import MaxTabsReachedError
        stub = Mock(spec=["ensure_and_fetch"])
        stub.ensure_and_fetch.side_effect = MaxTabsReachedError("Max tabs reached")
        flaresolverr._tab_mgr["service"] = stub
        res = self.app.get('/websocket_messages?url=https://x.io', expect_errors=True)
        self.assertEqual(res.status_code, 429)
        self.assertEqual(res.json["error"], "Max listeners reached")

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true'}, clear=False)
    def test_tab_manager_capped_new_url_returns_429_no_boot_thread(self):
        # A REAL FrameRouterService over a manager already at its primary cap:
        # ensure_and_fetch must raise synchronously (no doomed boot thread), so
        # the flag-on endpoint returns 429 instead of a permanent 200 "starting".
        from chrome_manager import MaxTabsReachedError
        from frame_router_service import FrameRouterService

        class CappedMgr:
            max_tabs = 2

            def __init__(self):
                self._url_index = {"https://a.io": "t1", "https://b.io": "t2"}
                self.booted = []

            def get_tab(self, url):
                return None

            def get_primary_tab(self, url):
                return None

            def ensure_can_create_primary(self, url):
                if len(self._url_index) >= self.max_tabs:
                    raise MaxTabsReachedError(f"Max tabs ({self.max_tabs}) reached")

            def create_tab(self, url):
                self.booted.append(url)

        flaresolverr._tab_mgr["service"] = FrameRouterService(CappedMgr(), Mock())
        res = self.app.get('/websocket_messages?url=https://c.io', expect_errors=True)
        self.assertEqual(res.status_code, 429)
        self.assertEqual(res.json["error"], "Max listeners reached")
        self.assertEqual(flaresolverr._tab_mgr["service"].mgr.booted, [])


class TestTabManagerRecycleCooldown(unittest.TestCase):
    """B3: _recycle_tab is single-flighted against restart/recover and per-URL
    cooldowned so a zombie + max_lifetime hit on the same tick can't spawn a
    leaked running shadow for the same URL."""

    def _failing(self):
        from chrome_manager import TabState
        from collections import deque
        cm = Mock()
        cm._restart_lock = threading.Lock()
        tab = TabState(tab_id="t1", url="https://x.io", tab=Mock(), target_id="t",
                       frame_buffer=deque())
        tab.status = "starting"
        cm.get_tab.return_value = tab
        cm.warm_tab.return_value = tab
        router = Mock()
        router.handoff.side_effect = RuntimeError("boom")
        return cm, router

    def test_cooldown_prevents_same_url_double_submit(self):
        url = "https://x.io"
        flaresolverr._recycle_cooldown_until.clear()
        cm, router = self._failing()
        try:
            flaresolverr._recycle_tab(cm, router, url, "zombie")
            # First recycle failed -> cooldown armed (NOT cleared on failure).
            self.assertIn(url, flaresolverr._recycle_cooldown_until)
            warm_calls = cm.warm_tab.call_count
            # Second recycle of the SAME url on the same tick must be skipped.
            flaresolverr._recycle_tab(cm, router, url, "max_lifetime")
            self.assertEqual(cm.warm_tab.call_count, warm_calls)
            # _restart_lock released after each call (no deadlock on retries).
            self.assertFalse(cm._restart_lock.locked())
        finally:
            flaresolverr._recycle_cooldown_until.clear()

    def test_single_flight_skips_when_restart_in_progress(self):
        from chrome_manager import TabState
        from collections import deque
        url = "https://x.io"
        flaresolverr._recycle_cooldown_until.clear()
        cm = Mock()
        cm._restart_lock = threading.Lock()
        cm._restart_lock.acquire(blocking=False)   # simulate restart/recover in progress
        router = Mock()
        try:
            flaresolverr._recycle_tab(cm, router, url, "zombie")
            # warm_tab never called while restart_lock is held.
            self.assertEqual(cm.warm_tab.call_count, 0)
        finally:
            cm._restart_lock.release()
            flaresolverr._recycle_cooldown_until.clear()


class TestTabManagerMaintenance(unittest.TestCase):
    """Self-heal of a dead manager (quorum C3) and the all-stale shared-browser
    watchdog (quorum C1 + user directive: restart ONLY when all listeners stale)."""

    def setUp(self):
        flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
        flaresolverr._recycle_cooldown_until.clear()
        flaresolverr._browser_restart_cooldown_until = 0.0
        import metrics as m

    def tearDown(self):
        flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
        flaresolverr._recycle_cooldown_until.clear()
        flaresolverr._browser_restart_cooldown_until = 0.0

    def _goal(self, running=True, loop_alive=True):
        cm = Mock()
        cm._running = running
        if loop_alive:
            class _Loop:
                def is_closed(self):
                    return False
                def is_running(self):
                    return True
            cm._loop = _Loop()
        else:
            cm._loop = None
        return cm

    def test_manager_broken_loop_dead(self):
        # A manager whose loop stopped/was wound down is "broken"; a healthy one
        # is not; a not-constructed one (None) is never "broken".
        self.assertFalse(flaresolverr._manager_broken(self._goal(running=True, loop_alive=True)))
        self.assertTrue(flaresolverr._manager_broken(self._goal(running=False, loop_alive=True)))
        cm = self._goal(running=True, loop_alive=True)
        cm._loop = None
        self.assertTrue(flaresolverr._manager_broken(cm))
        # Not constructed yet -> not broken (never discard a stub/injected singleton).
        self.assertFalse(flaresolverr._manager_broken(None))

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true'}, clear=False)
    def test_ensure_tab_manager_rebuilds_dead_manager(self):
        # A cached manager whose loop is dead must be discarded so a fresh manager
        # is built (never serve a permanently-wedged singleton). Patch the manager
        # construction + start so no real Chrome launches.
        class _Loop:
            def is_closed(self):
                return False
            def is_running(self):
                return True

        class _HealthyCM:
            def __init__(self, max_tabs):
                self.max_tabs = max_tabs
                self._running = True
                self._loop = _Loop()
            def start(self):
                pass

        with patch("chrome_manager.ChromeManager", _HealthyCM):
            # Inject a broken manager + service.
            broken = self._goal(running=False)
            old_service = Mock()
            flaresolverr._tab_mgr["cm"] = broken
            flaresolverr._tab_mgr["service"] = old_service
            svc = flaresolverr._ensure_tab_manager()
            # Singleton was rebuilt with a fresh, healthy manager.
            self.assertIsNotNone(svc)
            self.assertIsNot(flaresolverr._tab_mgr["service"], old_service)
            self.assertIsInstance(flaresolverr._tab_mgr["cm"], _HealthyCM)
            self.assertTrue(flaresolverr._tab_mgr["cm"]._running)
            self.assertFalse(flaresolverr._manager_broken(flaresolverr._tab_mgr["cm"]))

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'false'}, clear=False)
    def test_manager_broken_none_not_discarded(self):
        # With the flag off and nothing constructed, _ensure_tab_manager returns
        # None (legacy path) without building or discarding anything.
        self.assertIsNone(flaresolverr._ensure_tab_manager())

    def _maintenance_ctx(self, url_index):
        class _Loop:
            def is_closed(self):
                return False
            def is_running(self):
                return True
        cm = Mock()
        cm._lock = threading.RLock()
        cm._url_index = url_index
        cm._tabs = {}
        cm._loop_thread = None
        cm._running = True
        cm._loop = _Loop()
        cm.get_memory_usage_gb.return_value = 0.1
        cm.get_primary_tab.return_value = None   # skip max-lifetime body
        router = Mock()
        flaresolverr._tab_mgr["cm"] = cm
        flaresolverr._tab_mgr["router"] = router
        flaresolverr._tab_mgr["service"] = Mock()
        flaresolverr._browser_restart_cooldown_until = 0.0
        return cm

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true'}, clear=False)
    def test_maintenance_restarts_browser_when_ALL_listeners_stale(self):
        # Quorum C1 + user directive: the full browser restart fires only when
        # EVERY primary listener is stale (shared Chrome presumed dead). Here both
        # primaries are stale -> schedule a full restart, not per-url recycles.
        cm = self._maintenance_ctx({"a": "ta", "b": "tb"})
        STALE = time.time() - 999
        flaresolverr._tab_mgr["router"].get_last_frame_ts.side_effect = lambda u: STALE
        try:
            flaresolverr._tab_manager_maintenance()
            # A full restart (all_stale) is scheduled on the recycle worker.
            args = cm.schedule_recycling.call_args.args
            self.assertIs(args[2], flaresolverr._restart_all_tabs)
            self.assertEqual(args[1], "all_stale")
        finally:
            flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
            flaresolverr._browser_restart_cooldown_until = 0.0

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true'}, clear=False)
    def test_maintenance_recycles_single_stale_url_not_full_restart(self):
        # Only ONE of the two primaries is stale -> recycle just that url (the
        # shared browser is fine), NOT a full browser restart.
        cm = self._maintenance_ctx({"a": "ta", "b": "tb"})
        now = time.time()
        flaresolverr._tab_mgr["router"].get_last_frame_ts.side_effect = (
            lambda u: now - 999 if u == "a" else now - 1)
        try:
            flaresolverr._tab_manager_maintenance()
            # Only the stale url gets a per-url zombie recycle.
            scheduled = [c.args for c in cm.schedule_recycling.call_args_list]
            self.assertEqual(len(scheduled), 1)
            self.assertIs(scheduled[0][2], flaresolverr._recycle_tab)
            self.assertEqual(scheduled[0][5], "a")
            self.assertEqual(scheduled[0][6], "zombie")
        finally:
            flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
            flaresolverr._browser_restart_cooldown_until = 0.0


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


class TestListenerCorrectnessFixes(unittest.TestCase):
    """Ownership-safe _url_index pops + zero-loss reconnect."""

    def setUp(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def _mk_listener(self, mgr, url, lid):
        lst = flaresolverr_service.WebSocketListener(listener_id=lid, url=url,
                                                     status="running")
        lst.session_id = f"ws_listener_{lid}"
        mgr.listeners[lid] = lst
        mgr._url_index[url] = lid
        return lst

    def test_destroy_shadow_keeps_primary_index(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        primary = self._mk_listener(mgr, "http://x", "p1")
        shadow = self._mk_listener(mgr, "http://x", "s1")
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True):
            mgr.destroy_listener("s1")
        self.assertEqual(mgr._url_index.get("http://x"), "p1")
        self.assertIn("p1", mgr.listeners)
        self.assertNotIn("s1", mgr.listeners)

    def test_destroy_primary_clears_its_own_index(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        primary = self._mk_listener(mgr, "http://x", "p1")
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True):
            mgr.destroy_listener("p1")
        self.assertNotIn("http://x", mgr._url_index)

    def test_start_failure_keeps_primary_index(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        self._mk_listener(mgr, "http://x", "p1")
        shadow = flaresolverr_service.WebSocketListener(
            listener_id="s1", url="http://x", status="starting")
        mgr.listeners["s1"] = shadow   # NOTE: shadow NOT indexed yet
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=Exception("boom")), \
             patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True), \
             patch.object(flaresolverr_service.utils, "kill_orphaned_chrome"):
            with self.assertRaises(Exception):
                mgr._start_session(shadow, "http://x", 500)
        self.assertEqual(mgr._url_index.get("http://x"), "p1")
        self.assertNotIn("s1", mgr.listeners)

    def test_reconnect_preserves_buffered_messages(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        lst = self._mk_listener(mgr, "http://x", "p1")
        old_session = _make_mock_session()
        old_session.websocket_messages.append(
            flaresolverr_service.WebsocketMessage(
                timestamp=1.0, type="webSocketFrameReceived",
                url="http://x", payload="keepme"))
        flaresolverr_service.SESSIONS_STORAGE.sessions["ws_listener_p1"] = old_session

        captured = {}

        def fake_create(session_id=None, target_url="", **kw):
            captured["session_id"] = session_id
            captured["target_url"] = target_url
            new_session = _make_mock_session()
            new_session.session_id = session_id
            flaresolverr_service.SESSIONS_STORAGE.sessions[session_id] = new_session
            return new_session, True

        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=fake_create), \
             patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True) as m_destroy, \
             patch("flaresolverr_service._register_listener_cdp"), \
             patch("flaresolverr_service.func_timeout") as m_ft:
            mgr._reconnect_listener(lst)
        # old buffer content survived into the recreated session
        new_session = flaresolverr_service.SESSIONS_STORAGE.sessions["ws_listener_p1"]
        payloads = [m.payload for m in new_session.websocket_messages]
        self.assertEqual(payloads, ["keepme"])
        # destroy called BEFORE create (same-session-id recreation)
        self.assertGreaterEqual(m_destroy.call_count, 1)  # did not raise
        self.assertEqual(captured["target_url"], "http://x")

    def test_reconnect_attempts_reset_on_recovery(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        lst = self._mk_listener(mgr, "http://x", "p1")
        lst.reconnect_attempts = 3
        session = _make_mock_session()
        flaresolverr_service.SESSIONS_STORAGE.sessions["ws_listener_p1"] = session
        mgr.cleanup_stale()   # healthy probe -> recovery branch
        self.assertEqual(lst.reconnect_attempts, 0)
        self.assertEqual(lst.status, "running")


class TestListenerRecycle(unittest.TestCase):

    def setUp(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def _install_primary(self, mgr, url, lid, msgs=(), age_minutes=0):
        lst = flaresolverr_service.WebSocketListener(
            listener_id=lid, url=url, status="running")
        lst.session_id = f"ws_listener_{lid}"
        if age_minutes:
            lst.created_at = datetime.now() - timedelta(minutes=age_minutes)
        sess = _make_mock_session()
        sess.websocket_messages = deque(msgs, maxlen=500)
        flaresolverr_service.SESSIONS_STORAGE.sessions[lst.session_id] = sess
        mgr.listeners[lid] = lst
        mgr._url_index[url] = lid
        return lst

    def _fake_create(self, store, prefix="new"):
        state = {"n": 0}

        def fake(session_id=None, target_url="", **kw):
            state["n"] += 1
            s = _make_mock_session()
            s.session_id = session_id
            s.target_url = target_url
            store[session_id] = s
            return s, True
        return fake, state

    def test_recycle_zero_message_loss_and_atomic_swap(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        msgs = [flaresolverr_service.WebsocketMessage(
                    timestamp=float(i), type="webSocketFrameReceived",
                    url="http://x", payload=f"m{i}") for i in range(3)]
        old = self._install_primary(mgr, "http://x", "old1", msgs)
        store = flaresolverr_service.SESSIONS_STORAGE.sessions
        fake, state = self._fake_create(store)

        def fake_get(url_arg):
            pass  # instant page load

        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=fake), \
             patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True) as m_destroy, \
             patch("flaresolverr_service._register_listener_cdp"), \
             patch("flaresolverr_service.func_timeout", side_effect=lambda t, f, a: f(*a)):
            mgr._recycle_listener(old)

        new_lid = mgr._url_index["http://x"]
        self.assertNotEqual(new_lid, "old1")
        self.assertNotIn("old1", mgr.listeners)
        self.assertIn(new_lid, mgr.listeners)
        new_lst = mgr.listeners[new_lid]
        self.assertEqual(new_lst.status, "running")
        # zero message loss: all three survived into the new session
        new_sess = store[f"ws_listener_{new_lid}"]
        self.assertEqual([m.payload for m in new_sess.websocket_messages],
                         ["m0", "m1", "m2"])
        # old session was retired last
        m_destroy.assert_called_once_with("ws_listener_old1")
        # service continuity anchor copied
        self.assertIsNotNone(new_lst.service_started_at)
        self.assertEqual(new_lst.service_started_at, old.created_at)

    def test_recycled_successor_is_recyclable_again(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        old = self._install_primary(mgr, "http://x", "old1")
        store = flaresolverr_service.SESSIONS_STORAGE.sessions
        fake, state = self._fake_create(store)

        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=fake), \
             patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True), \
             patch("flaresolverr_service._register_listener_cdp"), \
             patch("flaresolverr_service.func_timeout", side_effect=lambda t, f, a: f(*a)):
            mgr._recycle_listener(old)

        new_lid = mgr._url_index["http://x"]
        self.assertIn(new_lid, mgr.listeners)
        self.assertFalse(mgr.listeners[new_lid].replacing)

    def test_stale_recycle_thread_is_noop(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        old = self._install_primary(mgr, "http://x", "old1")
        store = flaresolverr_service.SESSIONS_STORAGE.sessions
        fake, state = self._fake_create(store)

        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=fake), \
             patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True), \
             patch("flaresolverr_service._register_listener_cdp"), \
             patch("flaresolverr_service.func_timeout", side_effect=lambda t, f, a: f(*a)):
            mgr._recycle_listener(old)

        successor_lid = mgr._url_index["http://x"]
        n_listeners_after_first = len(mgr.listeners)

        # Simulate a stale thread arriving after the recycle completed.
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create") as m_create:
            mgr._recycle_listener(old)
        m_create.assert_not_called()
        # index still points at the first successor; no duplicate added
        self.assertEqual(mgr._url_index["http://x"], successor_lid)
        self.assertEqual(len(mgr.listeners), n_listeners_after_first)

    def test_recycle_boot_failure_keeps_original(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        old = self._install_primary(mgr, "http://x", "old1")
        store = flaresolverr_service.SESSIONS_STORAGE.sessions

        def failing_create(session_id=None, target_url="", **kw):
            raise Exception("chrome exploded")

        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=failing_create), \
             patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True), \
             patch.object(flaresolverr_service.utils, "kill_orphaned_chrome"):
            mgr._recycle_listener(old)
        # original untouched and still primary
        self.assertEqual(mgr._url_index["http://x"], "old1")
        self.assertIn("old1", mgr.listeners)
        self.assertEqual(old.status, "running")

    def test_recycle_single_flight(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        self.assertTrue(mgr._recycle_lock.acquire(blocking=False))
        old = self._install_primary(mgr, "http://x", "old1")
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create") as m_create:
            mgr._recycle_listener(old)
            m_create.assert_not_called()
        self.assertIn("old1", mgr.listeners)

    def test_cleanup_stale_triggers_recycle_for_old_primary_only(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        self._install_primary(mgr, "http://x", "young", age_minutes=10)
        old = self._install_primary(mgr, "http://y", "aged", age_minutes=200)
        with patch.object(mgr, "_recycle_listener") as m_recycle:
            mgr.cleanup_stale()
        m_recycle.assert_called_once()
        self.assertIs(m_recycle.call_args[0][0], old)

    def test_max_lifetime_zero_disables_recycle(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        self._install_primary(mgr, "http://x", "old1", age_minutes=200)
        with patch.dict('os.environ', {'WS_LISTENER_MAX_LIFETIME_MINUTES': '0'}), \
             patch.object(mgr, "_recycle_listener") as m_recycle:
            mgr.cleanup_stale()
        m_recycle.assert_not_called()
        # listener still alive and primary
        self.assertEqual(mgr._url_index.get("http://x"), "old1")
        self.assertIn("old1", mgr.listeners)

    def test_shadow_skipped_by_cleanup(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        shadow = flaresolverr_service.WebSocketListener(
            listener_id="sh1", url="http://x", status="starting", replacing=True)
        shadow.session_id = "ws_listener_sh1"
        shadow.created_at = datetime.now() - timedelta(hours=99)
        mgr.listeners["sh1"] = shadow   # deliberately NOT indexed
        with patch.object(mgr, "_recycle_listener") as m_recycle, \
             patch.object(mgr, "destroy_listener") as m_destroy:
            mgr.cleanup_stale()
        m_recycle.assert_not_called()
        m_destroy.assert_not_called()


class TestRecycleMetricInvisibility(unittest.TestCase):

    def setUp(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()

    def _gauge_val(self, gauge, labels=None):
        val = list(gauge.collect())[0].samples
        for s in val:
            if all(s.labels[k] == v for k, v in (labels or {}).items()):
                return s.value
        return None

    def test_gauges_exclude_shadow_during_overlap(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        primary = flaresolverr_service.WebSocketListener(
            listener_id="p1", url="http://x", status="running")
        mgr.listeners["p1"] = primary
        mgr._url_index["http://x"] = "p1"
        shadow = flaresolverr_service.WebSocketListener(
            listener_id="s1", url="http://x", status="starting", replacing=True)
        mgr.listeners["s1"] = shadow   # exists during boot, NOT indexed
        mgr._update_gauges()
        self.assertEqual(self._gauge_val(metrics.WS_LISTENERS_ACTIVE), 1.0)
        self.assertEqual(self._gauge_val(metrics.WS_LISTENERS_RUNNING), 1.0)
        self.assertEqual(
            self._gauge_val(metrics.WS_LISTENERS_STATUS, {"status": "starting"}),
            0.0)

    def test_uptime_uses_service_started_at(self):
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        lst = flaresolverr_service.WebSocketListener(
            listener_id="p1", url="http://x", status="running")
        lst.created_at = datetime.now()          # fresh browser...
        lst.service_started_at = datetime.now() - timedelta(hours=5)  # ...long service
        mgr.listeners["p1"] = lst
        mgr._url_index["http://x"] = "p1"
        mgr._update_per_url_metrics()
        uptime = self._gauge_val(metrics.WS_LISTENER_UPTIME, {"url": "http://x"})
        self.assertGreater(uptime, 17000)  # ~5h in seconds

    def test_final_redrain_preserves_fifo_order(self):
        """Frames landing between the swap merge and the final re-drain must be
        appended AFTER already-merged frames (FIFO), not prepended."""
        mgr = flaresolverr_service.WebSocketListenerManager(max_listeners=5)
        msgs = [flaresolverr_service.WebsocketMessage(
                    timestamp=float(i), type="webSocketFrameReceived",
                    url="http://x", payload=f"m{i}") for i in range(3)]
        old = flaresolverr_service.WebSocketListener(
            listener_id="old1", url="http://x", status="running")
        old.session_id = "ws_listener_old1"
        sess = _make_mock_session()

        fired = {"n": 0}

        class HookedDeque(deque):
            def clear(self_inner):
                super(HookedDeque, self_inner).clear()
                if fired["n"] == 0:  # fire once: after the main swap merge
                    fired["n"] += 1
                    self_inner.append(flaresolverr_service.WebsocketMessage(
                        timestamp=99.0, type="webSocketFrameReceived",
                        url="http://x", payload="late"))

        sess.websocket_messages = HookedDeque(msgs, maxlen=500)
        flaresolverr_service.SESSIONS_STORAGE.sessions[old.session_id] = sess
        mgr.listeners["old1"] = old
        mgr._url_index["http://x"] = "old1"
        store = flaresolverr_service.SESSIONS_STORAGE.sessions

        def fake(session_id=None, target_url="", **kw):
            s = _make_mock_session()
            s.session_id = session_id
            store[session_id] = s
            return s, True

        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                          side_effect=fake), \
             patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy",
                          return_value=True), \
             patch("flaresolverr_service._register_listener_cdp"), \
             patch("flaresolverr_service.func_timeout",
                   side_effect=lambda t, f, a: f(*a)):
            mgr._recycle_listener(old)

        new_lid = mgr._url_index["http://x"]
        new_sess = store[f"ws_listener_{new_lid}"]
        self.assertEqual([m.payload for m in new_sess.websocket_messages],
                         ["m0", "m1", "m2", "late"])


class TestReactorHygiene(unittest.TestCase):

    def _mk_reactor(self, handlers=None):
        r = reactor.Reactor.__new__(reactor.Reactor)   # skip __init__ (no Chrome)
        r.driver = SimpleNamespace(_delay=0)
        r.loop = Mock()
        r.loop.run_until_complete = Mock(return_value=None)
        r.lock = threading.Lock()
        r.event = threading.Event()
        r.handlers = handlers or {}
        return r

    def test_handler_exception_does_not_abort_batch(self):
        import asyncio as aio
        called = []
        outcomes = [RuntimeError("boom"), "ok"]
        def flaky(msg):
            o = outcomes.pop(0)
            if isinstance(o, Exception):
                raise o
            called.append(msg)
        r = self._mk_reactor(handlers={'*': flaky})
        entries = [{'message': json.dumps({'message': {'method': 'M1'}})},
                   {'message': json.dumps({'message': {'method': 'M2'}})}]
        calls = {'n': 0}
        def fake_get_log(name):
            calls['n'] += 1
            if calls['n'] >= 2:
                r.event.set()
            return entries
        r.driver = SimpleNamespace(_delay=0, get_log=fake_get_log)
        loop = aio.new_event_loop()
        try:
            loop.run_until_complete(aio.wait_for(r.listen(), timeout=5))
        finally:
            loop.close()
        self.assertEqual(len(called), 1)   # M2 still delivered after M1 raised

    def test_reactor_run_closes_loop_in_finally(self):
        r = self._mk_reactor()
        r.loop.run_until_complete.side_effect = RuntimeError("listen died")
        with patch('asyncio.set_event_loop'):
            r.run()   # must not raise
        r.loop.shutdown_asyncgens.assert_called_once()
        r.loop.shutdown_default_executor.assert_called_once()
        r.loop.close.assert_called_once()

    def test_dispatch_is_synchronous(self):
        src = inspect.getsource(reactor.Reactor.listen)
        self.assertNotIn('run_in_executor', src)


class TestLifecycleGauges(unittest.TestCase):

    def test_update_lifecycle_gauges_sets_values(self):
        import asyncio as aio
        worker = threading.Thread(target=lambda: time.sleep(1),
                                  name='ThreadPoolExecutor-0_worker')
        worker.start()
        loop_holder = aio.new_event_loop()   # held alive for the whole call
        try:
            flaresolverr.update_lifecycle_gauges()
            threads_val = list(metrics.PROCESS_THREADS_ACTIVE.collect())[0].samples[0].value
            self.assertGreaterEqual(threads_val, 1)
            workers_val = list(metrics.THREAD_POOL_WORKERS.collect())[0].samples[0].value
            self.assertGreaterEqual(workers_val, 1)
            loops_val = list(metrics.GC_EVENT_LOOPS.collect())[0].samples[0].value
            self.assertGreaterEqual(loops_val, 1)
        finally:
            loop_holder.close()
            worker.join(5)
        # GC_CHROME_DRIVERS: gauge series present (no real Chrome constructible here)
        chrome_samples = list(metrics.GC_CHROME_DRIVERS.collect())[0].samples
        self.assertGreater(len(chrome_samples), 0)
        self.assertGreaterEqual(chrome_samples[0].value, 0)
        # PROCESS_RSS_BYTES: series present, non-negative
        rss_samples = list(metrics.PROCESS_RSS_BYTES.collect())[0].samples
        self.assertGreater(len(rss_samples), 0)
        self.assertGreaterEqual(rss_samples[0].value, 0)

    def test_unquit_chrome_drivers_gauge(self):
        # NOTE: SimpleNamespace lacks __weakref__ on CPython 3.14; the fake
        # must be a plain-class instance so WeakSet.add() succeeds.
        class FakeChrome:
            pass

        fake = FakeChrome()
        uc_init.LIVE_CHROMES.add(fake)
        try:
            flaresolverr.update_lifecycle_gauges()
            val = list(metrics.UNQUIT_CHROME_DRIVERS.collect())[0].samples[0].value
            self.assertGreaterEqual(val, 1.0)
        finally:
            uc_init.LIVE_CHROMES.discard(fake)


class TestChromeRetention(unittest.TestCase):

    def test_finalize_pin_removed_and_registration_moved_to_end(self):
        init_src = inspect.getsource(uc_init.Chrome.__init__)
        self.assertNotIn('finalize(self,', init_src)
        self.assertIn('LIVE_CHROMES.add(self)', init_src)
        # registration must come AFTER self.options assignment (hash dependency)
        self.assertGreater(init_src.index('LIVE_CHROMES.add(self)'),
                           init_src.index('self.options'))

    def test_quit_discards_from_live_chromes(self):
        qsrc = inspect.getsource(uc_init.Chrome.quit)
        self.assertIn('LIVE_CHROMES.discard(self)', qsrc)

    def test_quit_severs_reactor_and_options_backref(self):
        qsrc = inspect.getsource(uc_init.Chrome.quit)
        self.assertIn('handlers.clear()', qsrc)
        self.assertIn('self.reactor = None', qsrc)
        self.assertIn("_session", qsrc)

    def test_quit_clears_handler_closures_and_severs(self):
        handlers = Mock()
        options = SimpleNamespace(_session='SENTINEL')
        chrome = SimpleNamespace(
            _quitted=False,
            reactor=SimpleNamespace(event=SimpleNamespace(set=Mock()),
                                    is_alive=Mock(return_value=False),
                                    join=Mock(),
                                    handlers=handlers),
            options=options,
            service=None,
            browser_pid=None,          # None -> os.kill guarded by except
            keep_user_data_dir=True,
            user_data_dir='',
            patcher=None)
        with patch('undetected_chromedriver.os.kill'):
            uc_init.Chrome.quit(chrome)
        handlers.clear.assert_called_once()
        self.assertIsNone(chrome.reactor)
        self.assertFalse(hasattr(options, '_session'))

    def test_quit_failure_is_retryable(self):
        # _quitted must only be set at the END of quit(), so a failed
        # attempt can be retried instead of being silently swallowed.
        chrome = SimpleNamespace(
            reactor=SimpleNamespace(event=SimpleNamespace(
                set=Mock(side_effect=RuntimeError("boom"))),
                is_alive=Mock(return_value=False),
                join=Mock(),
                handlers=Mock()),
            options=None,
            service=None,
            browser_pid=None,
            keep_user_data_dir=True,
            user_data_dir='',
            patcher=None)
        with patch('undetected_chromedriver.os.kill'):
            with self.assertRaises(RuntimeError):
                uc_init.Chrome.quit(chrome)
        self.assertIsNone(getattr(chrome, '_quitted', None))

    def test_quit_sets_quitted_on_success(self):
        chrome = SimpleNamespace(
            reactor=SimpleNamespace(event=SimpleNamespace(set=Mock()),
                                    is_alive=Mock(return_value=False),
                                    join=Mock(),
                                    handlers=Mock()),
            options=None,
            service=None,
            browser_pid=None,
            keep_user_data_dir=True,
            user_data_dir='',
            patcher=None)
        with patch('undetected_chromedriver.os.kill'):
            uc_init.Chrome.quit(chrome)
        self.assertTrue(chrome._quitted)

    def test_kill_unquit_chromes_calls_ensure_close(self):
        # NOTE: SimpleNamespace lacks __weakref__ on CPython 3.14, so a
        # WeakSet.add(fake) would raise TypeError; use a plain class fake.
        class FakeChrome:
            pass

        fake = FakeChrome()
        fake._ensure_close = Mock()
        uc_init.LIVE_CHROMES.add(fake)
        try:
            uc_init._kill_unquit_chromes()
            fake._ensure_close.assert_called_once_with(fake)
        finally:
            uc_init.LIVE_CHROMES.discard(fake)

    def test_kill_unquit_chromes_swallows_errors(self):
        class FakeChrome:
            pass

        fake = FakeChrome()
        fake._ensure_close = Mock(side_effect=RuntimeError("x"))
        uc_init.LIVE_CHROMES.add(fake)
        try:
            uc_init._kill_unquit_chromes()   # must not raise
        finally:
            uc_init.LIVE_CHROMES.discard(fake)


class TestThreeTierStallEscalation(unittest.TestCase):
    def setUp(self):
        flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
        flaresolverr._recycle_cooldown_until.clear()
        flaresolverr._browser_restart_cooldown_until = 0.0
        flaresolverr._restart_cooldown_until = 0.0

    def tearDown(self):
        flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
        flaresolverr._recycle_cooldown_until.clear()
        flaresolverr._browser_restart_cooldown_until = 0.0
        flaresolverr._restart_cooldown_until = 0.0

    def _setup_context(self, tabs_dict):
        from chrome_manager import TabState
        cm = Mock()
        cm._lock = threading.RLock()
        cm._url_index = {tab.url: tab.tab_id for tab in tabs_dict.values()}
        cm._tabs = {tab.tab_id: tab for tab in tabs_dict.values()}
        cm._loop_thread = None
        cm._running = True
        class _Loop:
            def is_closed(self):
                return False
            def is_running(self):
                return True
        cm._loop = _Loop()
        cm.get_memory_usage_gb.return_value = 0.1
        cm.get_primary_tab.side_effect = lambda u: tabs_dict.get(u)
        cm.soft_reload_tab.return_value = True

        router = Mock()
        router.get_last_frame_ts.side_effect = lambda u: tabs_dict[u].last_frame_ts if u in tabs_dict else None
        flaresolverr._tab_mgr["cm"] = cm
        flaresolverr._tab_mgr["router"] = router
        flaresolverr._tab_mgr["service"] = Mock()
        return cm, router

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true', 'WS_DATA_STALL_TIMEOUT_S': '60.0'}, clear=False)
    def test_tier1_soft_reload_on_stall_consecutive_zero(self):
        from chrome_manager import TabState
        url = "https://mevx.io/?chain=solana"
        tab = TabState(
            tab_id="tab_1",
            url=url,
            tab=Mock(),
            target_id="t1",
            status="running",
            service_started_at=datetime.now() - timedelta(seconds=300),
            last_data_frame_ts=time.time() - 100.0,
            consecutive_stalls=0
        )
        cm, router = self._setup_context({url: tab})
        
        tier1_val_before = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="1")._value.get()
        flaresolverr._tab_manager_maintenance()
        tier1_val_after = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="1")._value.get()

        cm.soft_reload_tab.assert_called_once_with("tab_1")
        self.assertEqual(tier1_val_after - tier1_val_before, 1)
        self.assertEqual(tab.consecutive_stalls, 1)

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true', 'WS_DATA_STALL_TIMEOUT_S': '60.0'}, clear=False)
    def test_tier1_soft_reload_failure_escalates_to_tier2(self):
        from chrome_manager import TabState
        url = "https://mevx.io/?chain=solana"
        tab = TabState(
            tab_id="tab_1",
            url=url,
            tab=Mock(),
            target_id="t1",
            status="running",
            service_started_at=datetime.now() - timedelta(seconds=300),
            last_data_frame_ts=time.time() - 100.0,
            consecutive_stalls=0
        )
        cm, router = self._setup_context({url: tab})
        cm.soft_reload_tab.return_value = False

        tier1_before = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="1")._value.get()
        tier2_before = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="2")._value.get()
        flaresolverr._tab_manager_maintenance()
        tier1_after = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="1")._value.get()
        tier2_after = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="2")._value.get()

        self.assertEqual(tier1_after - tier1_before, 1)
        self.assertEqual(tier2_after - tier2_before, 1)
        self.assertEqual(tab.consecutive_stalls, 2)
        cm.schedule_recycling.assert_called_once_with(url, "stall_tier2", flaresolverr._recycle_tab, cm, router, url, "stall_tier2")

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true', 'WS_DATA_STALL_TIMEOUT_S': '60.0'}, clear=False)
    def test_tier2_tab_recycle_on_consecutive_stalls_one(self):
        from chrome_manager import TabState
        url = "https://mevx.io/?chain=solana"
        tab = TabState(
            tab_id="tab_1",
            url=url,
            tab=Mock(),
            target_id="t1",
            status="running",
            service_started_at=datetime.now() - timedelta(seconds=300),
            last_data_frame_ts=time.time() - 100.0,
            consecutive_stalls=1
        )
        cm, router = self._setup_context({url: tab})

        tier2_before = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="2")._value.get()
        flaresolverr._tab_manager_maintenance()
        tier2_after = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="2")._value.get()

        self.assertEqual(tier2_after - tier2_before, 1)
        self.assertEqual(tab.consecutive_stalls, 2)
        cm.schedule_recycling.assert_called_once_with(url, "stall_tier2", flaresolverr._recycle_tab, cm, router, url, "stall_tier2")

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true', 'WS_DATA_STALL_TIMEOUT_S': '60.0'}, clear=False)
    def test_tier3_standby_recycle_on_consecutive_stalls_two(self):
        from chrome_manager import TabState
        url = "https://mevx.io/?chain=solana"
        tab = TabState(
            tab_id="tab_1",
            url=url,
            tab=Mock(),
            target_id="t1",
            status="running",
            service_started_at=datetime.now() - timedelta(seconds=300),
            last_data_frame_ts=time.time() - 100.0,
            consecutive_stalls=2
        )
        cm, router = self._setup_context({url: tab})

        tier3_before = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="3")._value.get()
        flaresolverr._tab_manager_maintenance()
        tier3_after = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="3")._value.get()

        self.assertEqual(tier3_after - tier3_before, 1)
        self.assertEqual(tab.consecutive_stalls, 3)
        cm.schedule_recycling.assert_called_once_with(url, "stall_tier3", flaresolverr._recycle_browser_standby, cm, router, "stall_tier3")

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true', 'WS_DATA_STALL_TIMEOUT_S': '60.0'}, clear=False)
    def test_tier3_immediate_on_multiple_stalled_tabs(self):
        from chrome_manager import TabState
        url1 = "https://mevx.io/?chain=solana"
        url2 = "https://mevx.io/?chain=bsc"
        tab1 = TabState(
            tab_id="tab_1",
            url=url1,
            tab=Mock(),
            target_id="t1",
            status="running",
            service_started_at=datetime.now() - timedelta(seconds=300),
            last_data_frame_ts=time.time() - 100.0,
            consecutive_stalls=0
        )
        tab2 = TabState(
            tab_id="tab_2",
            url=url2,
            tab=Mock(),
            target_id="t2",
            status="running",
            service_started_at=datetime.now() - timedelta(seconds=300),
            last_data_frame_ts=time.time() - 100.0,
            consecutive_stalls=0
        )
        cm, router = self._setup_context({url1: tab1, url2: tab2})

        tier3_before1 = metrics.WS_STALL_ESCALATIONS.labels(url=url1, tier="3")._value.get()
        tier3_before2 = metrics.WS_STALL_ESCALATIONS.labels(url=url2, tier="3")._value.get()
        flaresolverr._tab_manager_maintenance()
        tier3_after1 = metrics.WS_STALL_ESCALATIONS.labels(url=url1, tier="3")._value.get()
        tier3_after2 = metrics.WS_STALL_ESCALATIONS.labels(url=url2, tier="3")._value.get()

        self.assertEqual(tier3_after1 - tier3_before1, 1)
        self.assertEqual(tier3_after2 - tier3_before2, 1)
        self.assertGreaterEqual(tab1.consecutive_stalls, 3)
        self.assertGreaterEqual(tab2.consecutive_stalls, 3)
        args = cm.schedule_recycling.call_args.args
        self.assertEqual(args[1], "stall_tier3")
        self.assertIs(args[2], flaresolverr._recycle_browser_standby)

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true', 'WS_DATA_STALL_TIMEOUT_S': '60.0'}, clear=False)
    def test_gmgn_grace_period_prevents_premature_stall(self):
        from chrome_manager import TabState
        url = "https://gmgn.ai/sol"
        # Tab created 100s ago (< 180s grace period), no data frame received yet
        tab = TabState(
            tab_id="tab_gmgn",
            url=url,
            tab=Mock(),
            target_id="tgmgn",
            status="running",
            service_started_at=datetime.now() - timedelta(seconds=100),
            last_data_frame_ts=0.0,
            consecutive_stalls=0
        )
        cm, router = self._setup_context({url: tab})

        tier1_before = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="1")._value.get()
        flaresolverr._tab_manager_maintenance()
        tier1_after = metrics.WS_STALL_ESCALATIONS.labels(url=url, tier="1")._value.get()

        self.assertEqual(tier1_after, tier1_before)
        cm.soft_reload_tab.assert_not_called()
        cm.schedule_recycling.assert_not_called()


class TestStandbyBrowserSwapIntegration(unittest.TestCase):
    def setUp(self):
        flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
        flaresolverr._recycle_cooldown_until.clear()
        flaresolverr._browser_restart_cooldown_until = 0.0
        flaresolverr._restart_cooldown_until = 0.0

    def tearDown(self):
        flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
        flaresolverr._recycle_cooldown_until.clear()
        flaresolverr._browser_restart_cooldown_until = 0.0
        flaresolverr._restart_cooldown_until = 0.0

    def test_standby_zero_drop_swap_under_simulated_load(self):
        from chrome_manager import ChromeManager, TabState
        from frame_router import FrameRouter
        import flaresolverr_service as fs

        mgr = ChromeManager(max_tabs=5)
        mgr._loop = Mock()
        router = FrameRouter(mgr)

        url1 = "https://mevx.io/?chain=solana"
        url2 = "https://gmgn.ai/sol"

        # Mock old browser
        old_browser = Mock()
        old_browser.stop = Mock()
        mgr._browser = old_browser
        old_dir = "/tmp/tabmgr_old_load_test"
        mgr._shared_browser_user_data_dir = old_dir
        fs.register_shared_browser_dir(old_dir)

        old_tab1 = TabState(tab_id="old_t1", url=url1, tab=Mock(), target_id="ot1", status="running")
        old_tab2 = TabState(tab_id="old_t2", url=url2, tab=Mock(), target_id="ot2", status="running")
        mgr._tabs[old_tab1.tab_id] = old_tab1
        mgr._tabs[old_tab2.tab_id] = old_tab2
        mgr._url_index[url1] = old_tab1.tab_id
        mgr._url_index[url2] = old_tab2.tab_id

        # Stream messages into old tabs
        now = time.time()
        for i in range(10):
            p = f'{{"jsonrpc":"2.0","id":"msg_{i}","result":{{"i":{i}}}}}'
            old_tab1._handle_frame("webSocketFrameReceived", p, cdp_ts=100.0 + i)
            old_tab2._handle_frame("webSocketFrameReceived", f'{{"t":"callout_global","data":[{{"uid":"u_{i}"}}]}}', cdp_ts=100.0 + i)

        # Standby browser and tabs
        standby_browser = Mock()
        standby_browser.stop = Mock()
        standby_dir = "/tmp/tabmgr_standby_load_test"
        fs.register_shared_browser_dir(standby_dir)

        new_tab1 = TabState(tab_id="new_t1", url=url1, tab=Mock(), target_id="nt1", status="running")
        new_tab2 = TabState(tab_id="new_t2", url=url2, tab=Mock(), target_id="nt2", status="running")

        # Overlapping messages received during warm
        for i in range(8, 15):
            p = f'{{"jsonrpc":"2.0","id":"msg_{i}","result":{{"i":{i}}}}}'
            new_tab1._handle_frame("webSocketFrameReceived", p, cdp_ts=1.0 + i)
            new_tab2._handle_frame("webSocketFrameReceived", f'{{"t":"callout_global","data":[{{"uid":"u_{i}"}}]}}', cdp_ts=1.0 + i)

        warmed = {url1: new_tab1, url2: new_tab2}

        # Perform standby swap
        mgr.swap_standby_browser(standby_browser, standby_dir, warmed, router, quiescence_s=0.0)

        # Verify zero-drop buffer merge and dedup
        drained1 = mgr.drain_tab(url1)
        self.assertEqual(len(drained1), 15)  # 0..14 unique
        drained2 = mgr.drain_tab(url2)
        self.assertEqual(len(drained2), 15)  # 0..14 unique

        self.assertEqual(mgr._url_index[url1], "new_t1")
        self.assertEqual(mgr._url_index[url2], "new_t2")
        self.assertIs(mgr._browser, standby_browser)

    def test_recycle_browser_standby_cooldown_and_dispatch(self):
        cm = Mock()
        cm.recycle_browser_standby.return_value = True
        router = Mock()
        flaresolverr._tab_mgr["cm"] = cm
        flaresolverr._tab_mgr["router"] = router

        standby_before = metrics.WS_STANDBY_BROWSER_RECYCLES.labels(reason="stall_tier3")._value.get()
        flaresolverr._recycle_browser_standby(cm, router, "stall_tier3")
        standby_after = metrics.WS_STANDBY_BROWSER_RECYCLES.labels(reason="stall_tier3")._value.get()

        cm.recycle_browser_standby.assert_called_once_with(router, reason="stall_tier3")
        self.assertEqual(standby_after - standby_before, 1)
        self.assertGreater(flaresolverr._browser_restart_cooldown_until, time.time())
        self.assertEqual(flaresolverr._restart_cooldown_until, flaresolverr._browser_restart_cooldown_until)


class TestScheduledAndMemoryTriggers(unittest.TestCase):
    def setUp(self):
        flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
        flaresolverr._recycle_cooldown_until.clear()
        flaresolverr._browser_restart_cooldown_until = 0.0
        flaresolverr._restart_cooldown_until = 0.0
        flaresolverr._last_periodic_recycle_time = time.time()

    def tearDown(self):
        flaresolverr._tab_mgr = {"cm": None, "router": None, "service": None}
        flaresolverr._recycle_cooldown_until.clear()
        flaresolverr._browser_restart_cooldown_until = 0.0
        flaresolverr._restart_cooldown_until = 0.0

    def _setup_context(self, url="https://mevx.io/?chain=solana"):
        from chrome_manager import TabState
        tab = TabState(
            tab_id="tab_1",
            url=url,
            tab=Mock(),
            target_id="t1",
            status="running",
            service_started_at=datetime.now() - timedelta(seconds=100),
            last_data_frame_ts=time.time() - 10.0,
            consecutive_stalls=0
        )
        cm = Mock()
        cm._lock = threading.RLock()
        cm._url_index = {url: "tab_1"}
        cm._tabs = {"tab_1": tab}
        cm._loop_thread = None
        cm._running = True
        class _Loop:
            def is_closed(self):
                return False
            def is_running(self):
                return True
        cm._loop = _Loop()
        cm.get_primary_tab.return_value = tab

        router = Mock()
        router.get_last_frame_ts.return_value = time.time() - 10.0
        flaresolverr._tab_mgr["cm"] = cm
        flaresolverr._tab_mgr["router"] = router
        flaresolverr._tab_mgr["service"] = Mock()
        return cm, router, tab

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true', 'WS_CHROME_MAX_MEMORY_MB': '1200'}, clear=False)
    def test_memory_ceiling_triggers_standby_recycle(self):
        cm, router, tab = self._setup_context()
        cm.get_memory_usage_gb.return_value = 1.3  # > 1200MB ceiling, < 1.6GB emergency

        flaresolverr._tab_manager_maintenance()

        args = cm.schedule_recycling.call_args.args
        self.assertEqual(args[1], "memory")
        self.assertIs(args[2], flaresolverr._recycle_browser_standby)
        self.assertEqual(args[5], "memory")

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true', 'WS_CHROME_MAX_MEMORY_MB': '1200'}, clear=False)
    def test_memory_emergency_triggers_inplace_restart(self):
        cm, router, tab = self._setup_context()
        cm.get_memory_usage_gb.return_value = 1.8  # > 1.6GB emergency

        flaresolverr._tab_manager_maintenance()

        args = cm.schedule_recycling.call_args.args
        self.assertEqual(args[1], "emergency_memory")
        self.assertIs(args[2], flaresolverr._restart_all_tabs)
        self.assertEqual(args[5], "emergency_memory")

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true', 'WS_CHROME_RECYCLE_INTERVAL_HOURS': '6.0'}, clear=False)
    def test_periodic_timer_triggers_standby_recycle(self):
        cm, router, tab = self._setup_context()
        cm.get_memory_usage_gb.return_value = 0.2
        # Set last periodic recycle to 7 hours ago
        flaresolverr._last_periodic_recycle_time = time.time() - (7 * 3600.0)

        flaresolverr._tab_manager_maintenance()

        args = cm.schedule_recycling.call_args.args
        self.assertEqual(args[1], "scheduled")
        self.assertIs(args[2], flaresolverr._recycle_browser_standby)
        self.assertEqual(args[5], "scheduled")

    @patch.dict('os.environ', {'WS_TAB_MANAGER_ENABLED': 'true'}, clear=False)
    def test_rate_gauges_updated_on_cycle(self):
        cm, router, tab = self._setup_context()
        cm.get_memory_usage_gb.return_value = 0.2
        now = time.time()
        for i in range(5):
            tab.data_frame_history.append(now - i)
            tab.control_frame_history.append(now - i)

        flaresolverr._tab_manager_maintenance()

        data_rate = metrics.WS_DATA_FRAME_RATE.labels(url=tab.url)._value.get()
        ctrl_rate = metrics.WS_CONTROL_FRAME_RATE.labels(url=tab.url)._value.get()

        self.assertAlmostEqual(data_rate, 5.0 / 60.0, places=3)
        self.assertAlmostEqual(ctrl_rate, 5.0 / 60.0, places=3)


if __name__ == '__main__':
    unittest.main()

