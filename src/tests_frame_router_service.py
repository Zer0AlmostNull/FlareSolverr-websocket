# FlareSolverr-websocket/src/tests_frame_router_service.py
import json
import unittest
from unittest import mock
from collections import deque
import time

from frame_router_service import FrameRouterService, logger


class TestFrameRouterService(unittest.TestCase):
    def setUp(self):
        class FakeMgr:
            def __init__(self):
                self._tabs = {}
                self._lock = mock.Mock()
            def get_tab(self, url):
                return self._tabs.get(url)
            def ensure_can_create_primary(self, url):
                return None
            def create_tab(self, url):
                ts = mock.Mock()
                ts.status = "starting"
                ts.frame_buffer = deque(maxlen=2000)
                ts.lock = mock.Mock()
                ts.last_frame_ts = time.time()
                self._tabs[url] = ts
                return ts
            def drain_tab(self, url):
                return []

        class FakeRouter:
            def tab_status(self, url):
                return "running"
            def drain(self, url):
                return []

        mgr = FakeMgr()
        self.svc = FrameRouterService(mgr, FakeRouter())

    def test_response_schema(self):
        payload = self.svc.ensure_and_fetch("https://mevx.io/?chain=solana")
        # Contract: exactly these keys, typed as current WebsocketMessage.__dict__
        self.assertIn("status", payload)
        self.assertIn("messages", payload)
        self.assertIsInstance(payload["messages"], list)

    def test_keys_of_message_match_current_contract(self):
        # Populate a frame and assert it serializes to timestamp/type/url/payload
        from frame_router_service import _web_socket_message_dict
        msg = _web_socket_message_dict(
            timestamp=1.0, type="webSocketFrameReceived", url="u", payload="p")
        self.assertEqual(sorted(msg.keys()), ["payload", "timestamp", "type", "url"])
        self.assertIsInstance(msg["timestamp"], float)

    def test_ensure_and_fetch_strips_internal_cdp_ts(self):
        # cdp_ts is used only for dedup; it must NEVER leak into /websocket_messages.
        from frame_router import FrameRouter
        router = mock.Mock(spec=["tab_status", "drain"])
        router.tab_status.return_value = "running"
        router.drain.return_value = [
            {"timestamp": 1.0, "type": "webSocketFrameReceived",
             "url": "u", "payload": "p", "cdp_ts": 123.0}
        ]
        svc = FrameRouterService(mock.Mock(), router)
        out = svc.ensure_and_fetch("u")
        self.assertEqual(len(out["messages"]), 1)
        self.assertEqual(sorted(out["messages"][0].keys()),
                         ["payload", "timestamp", "type", "url"])
        self.assertNotIn("cdp_ts", out["messages"][0])

    def test_boot_tab_max_tabs_reached_logs_warning(self):
        # create_tab raises MaxTabsReachedError -> _boot_tab logs warning, clears pending
        from chrome_manager import MaxTabsReachedError
        mgr = mock.Mock()
        mgr.create_tab.side_effect = MaxTabsReachedError("Max tabs reached")
        router = mock.Mock()
        svc = FrameRouterService(mgr, router)
        with self.assertLogs(logger, level="WARNING") as cm:
            svc._boot_tab("https://example.com")
        self.assertIn("max tabs reached", cm.output[0].lower())
        # pending should be cleared even on MaxTabsReachedError
        self.assertNotIn("https://example.com", svc._pending)

    def test_new_url_at_primary_cap_raises_without_boot_thread(self):
        # Manager is already at its primary cap and a NEW url requests:
        # ensure_and_fetch must raise MaxTabsReachedError SYNCHRONOUSLY (the
        # background boot could only fail) and must NOT spawn a boot thread.
        from chrome_manager import MaxTabsReachedError

        class CappedMgr:
            max_tabs = 2

            def __init__(self):
                self._url_index = {"https://a.io": "t1", "https://b.io": "t2"}
                self.booted = []

            def get_tab(self, url):
                return None

            def ensure_can_create_primary(self, url):
                if len(self._url_index) >= self.max_tabs:
                    raise MaxTabsReachedError(f"Max tabs ({self.max_tabs}) reached")

            def create_tab(self, url):
                self.booted.append(url)

        mgr = CappedMgr()
        svc = FrameRouterService(mgr, mock.Mock())
        with self.assertRaises(MaxTabsReachedError):
            svc.ensure_and_fetch("https://new.io")
        self.assertEqual(mgr.booted, [])          # no doomed boot thread spawned
        self.assertNotIn("https://new.io", svc._pending)

    def test_new_url_with_free_slot_returns_starting_and_boots(self):
        # A spare primary slot must keep the first-use "starting" behaviour and
        # kick off the background boot.
        from chrome_manager import MaxTabsReachedError

        class FreeSlotMgr:
            max_tabs = 2

            def __init__(self):
                self._url_index = {"https://a.io": "t1"}   # one slot free
                self.booted = []

            def get_tab(self, url):
                return None

            def ensure_can_create_primary(self, url):
                if len(self._url_index) >= self.max_tabs:
                    raise MaxTabsReachedError(f"Max tabs ({self.max_tabs}) reached")

            def create_tab(self, url):
                self.booted.append(url)

        mgr = FreeSlotMgr()
        svc = FrameRouterService(mgr, mock.Mock())
        payload = svc.ensure_and_fetch("https://new.io")
        self.assertEqual(payload["status"], "starting")
        self.assertEqual(payload["messages"], [])
        for _ in range(50):
            if mgr.booted:
                break
            time.sleep(0.01)
        self.assertEqual(mgr.booted, ["https://new.io"])