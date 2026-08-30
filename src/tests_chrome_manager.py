import asyncio
import threading
import time
import unittest
from unittest import mock
from collections import deque

import chrome_manager
from chrome_manager import ChromeManager, TabState


class _FakeTab:
    """Minimal stand-in for a nodriver tab so _async_create_tab_registered's
    CDP-attach path (send/add_handler) can run without a real browser/loop."""
    target_id = "t"

    def __init__(self):
        self.handlers = []

    async def send(self, *a, **k):
        return None

    def add_handler(self, *a, **k):
        self.handlers.append(a)

    async def get(self, *a, **k):
        return None

    async def close(self):
        return None


class TestChromeManager(unittest.TestCase):
    """Logic-level tests. nodriver _launch_browser and _async_* coroutines are
    stubbed so no real Chrome / event loop is needed."""

    def _make_manager(self, max_tabs=3):
        mgr = ChromeManager(max_tabs=max_tabs)
        # Provide a real (temporary) event loop so _call actually runs the
        # _async_* coroutine bodies and builds/registers real TabState objects.
        mgr._loop = asyncio.new_event_loop()
        mgr._call = lambda coro, *a, **k: mgr._loop.run_until_complete(coro)
        # Fake browser: .get returns a fake tab; no real Chrome launch.
        fake_browser = mock.Mock()
        fake_browser.get = mock.AsyncMock(return_value=_FakeTab())
        mgr._browser = fake_browser
        mgr._running = True
        return mgr

    def tearDown(self):
        # _recover()/_start() register the shared browser's user_data_dir with the
        # orphan sweeper (a module global). Tests here may invoke recovery without a
        # matching stop(), so clear the global to avoid leaking into later suites
        # (e.g. tests_utils.py TestLiveUserDataDirs exact-set assertions).
        import flaresolverr_service as fs
        with fs._shared_dirs_lock:
            fs._shared_browser_dirs.clear()

    def test_create_tab_returns_tab_state(self):
        mgr = self._make_manager()
        tab = mgr.create_tab("https://mevx.io/?chain=solana")
        self.assertIsNotNone(tab)
        self.assertEqual(tab.url, "https://mevx.io/?chain=solana")
        self.assertEqual(tab.status, "running")

    def test_max_tabs_enforced(self):
        mgr = self._make_manager(max_tabs=2)
        mgr.create_tab("https://mevx.io/?chain=solana")
        mgr.create_tab("https://mevx.io/?chain=bsc")
        from chrome_manager import MaxTabsReachedError
        with self.assertRaises(MaxTabsReachedError):
            mgr.create_tab("https://mevx.io/?chain=eth")

    def test_ensure_can_create_primary_raises_at_primary_cap(self):
        # ensure_can_create_primary must reject a NEW url once the manager is at
        # its max_tabs primary cap (mirrors create_tab's non-shadow check), but
        # must allow shadows for already-served urls.
        from chrome_manager import MaxTabsReachedError
        mgr = self._make_manager(max_tabs=2)
        mgr.create_tab("https://mevx.io/?chain=solana")
        mgr.create_tab("https://mevx.io/?chain=bsc")
        with self.assertRaises(MaxTabsReachedError):
            mgr.ensure_can_create_primary("https://mevx.io/?chain=eth")
        # A url that already has a primary is a shadow create -> not capped.
        mgr.ensure_can_create_primary("https://mevx.io/?chain=solana")
        # A free slot keeps the new-url path open.
        mgr.retire_tab("https://mevx.io/?chain=bsc")
        mgr.ensure_can_create_primary("https://mevx.io/?chain=eth")

    def test_drain_tab_clears_buffer(self):
        mgr = self._make_manager()
        tab = mgr.create_tab("https://mevx.io/?chain=solana")
        tab._feed("webSocketFrameReceived", "p")
        self.assertEqual(len(mgr.drain_tab(tab.url)), 1)
        self.assertEqual(len(mgr.drain_tab(tab.url)), 0)

    def test_retire_tab_removes_registry_entry(self):
        mgr = self._make_manager()
        mgr.create_tab("https://mevx.io/?chain=solana")
        mgr.retire_tab("https://mevx.io/?chain=solana")
        self.assertIsNone(mgr.get_tab("https://mevx.io/?chain=solana"))

    def test_get_memory_usage_gb_returns_float(self):
        mgr = self._make_manager()
        self.assertIsInstance(mgr.get_memory_usage_gb(), float)

    def test_get_memory_usage_gb_reads_chrome_pid_not_python(self):
        # Regression (quorum): the memory gauge must read the REAL Chrome pid
        # (_process_pid), never /proc/self/statm (the Python process).
        from unittest import mock as _m

        class _MI:
            rss = 2_000_000_000

        class FakeProc:
            def __init__(self, pid):
                self.pid = pid
            def memory_info(self):
                return _MI()
            def children(self, recursive=True):
                return []

        mgr = self._make_manager()
        mgr._browser = _m.Mock()
        mgr._browser._process_pid = 999999
        with _m.patch("psutil.Process", side_effect=FakeProc) as mproc:
            gb = mgr.get_memory_usage_gb()
        self.assertAlmostEqual(gb, 2.0, places=1)
        # Queried the Chrome pid, not the Python process.
        self.assertEqual(mproc.call_args.args, (999999,))

    def test_get_memory_usage_gb_no_browser_returns_zero(self):
        mgr = self._make_manager()
        mgr._browser = None
        self.assertEqual(mgr.get_memory_usage_gb(), 0.0)

    def test_recover_recreates_primaries_after_loop_crash(self):
        from unittest import mock as _m
        mgr = self._make_manager(max_tabs=3)
        mgr.create_tab("https://mevx.io/?chain=solana")
        mgr.create_tab("https://mevx.io/?chain=bsc")
        # Simulate a crash: stale browser, then force recovery to relaunch.
        # _recover() internally creates a fresh loop and calls _launch_browser.
        new_browser = _m.Mock()
        new_browser.get = _m.AsyncMock(return_value=_FakeTab())
        with _m.patch("chrome_manager._launch_browser", _m.AsyncMock(return_value=new_browser)):
            ok = mgr._recover()
        self.assertTrue(ok)
        # Both primaries must be re-registered and RUNNING.
        self.assertEqual(set(mgr._url_index.keys()), {"https://mevx.io/?chain=solana", "https://mevx.io/?chain=bsc"})
        for url in mgr._url_index.keys():
            tab = mgr.get_primary_tab(url)
            self.assertIsNotNone(tab)
            self.assertEqual(tab.status, "running")
        # Stale browser reference should be replaced with the fresh one.
        self.assertIs(mgr._browser, new_browser)

    def test_recover_unregisters_old_shared_dir(self):
        # Regression (quorum A6/C12): after recovery the OLD shared user_data_dir
        # must be unregistered with the orphan sweeper so a still-alive pre-crash
        # Chrome can be reaped and its profile dir swept (previously it stayed
        # registered forever -> leaked process + profile dir).
        from unittest import mock as _m
        import flaresolverr_service as fs
        with fs._shared_dirs_lock:
            fs._shared_browser_dirs.clear()
        mgr = self._make_manager()
        old_dir = "/tmp/tabmgr_old_dir"
        new_dir = "/tmp/tabmgr_new_dir"
        fs.register_shared_browser_dir(old_dir)
        mgr._shared_browser_user_data_dir = old_dir
        new_browser = _m.Mock()
        new_browser.get = _m.AsyncMock(return_value=_FakeTab())
        with _m.patch("chrome_manager._launch_browser",
                      _m.AsyncMock(return_value=new_browser)) as launch:
            launch.user_data_dir = new_dir   # the fresh launch reports a new dir
            ok = mgr._recover()
        self.assertTrue(ok)
        with fs._shared_dirs_lock:
            self.assertNotIn(old_dir, fs._shared_browser_dirs)
            self.assertIn(new_dir, fs._shared_browser_dirs)

    def test_handoff_retire_keeps_url_active_and_uptime_continuous(self):
        from metrics import WS_LISTENERS_ACTIVE, WS_LISTENER_ACTIVE, WS_LISTENER_UPTIME
        mgr = self._make_manager()
        url = "https://mevx.io/?chain=handoff"
        primary = mgr.create_tab(url)
        self.assertEqual(WS_LISTENERS_ACTIVE._value.get(), 1)
        self.assertEqual(WS_LISTENER_ACTIVE.labels(url=url)._value.get(), 1)
        uptime_marker = WS_LISTENER_UPTIME.labels(url=url)._value.get()
        shadow = mgr.create_tab(url)
        # A shadow (warming) tab must re-affirm the gauge but NOT reset uptime.
        self.assertEqual(WS_LISTENER_ACTIVE.labels(url=url)._value.get(), 1)
        self.assertEqual(WS_LISTENER_UPTIME.labels(url=url)._value.get(), uptime_marker)
        self.assertEqual(WS_LISTENERS_ACTIVE._value.get(), 1)
        mgr.swap_primary(url, primary.tab_id, shadow.tab_id)
        mgr.retire_tab_id(primary.tab_id)
        # Old primary retired; the NEW primary is still live -> gauge stays 1,
        # uptime stays continuous, global count stays accurate.
        self.assertEqual(WS_LISTENER_ACTIVE.labels(url=url)._value.get(), 1)
        self.assertEqual(WS_LISTENER_UPTIME.labels(url=url)._value.get(), uptime_marker)
        self.assertEqual(WS_LISTENERS_ACTIVE._value.get(), 1)

    def test_stop_and_recreate_keeps_global_gauge_accurate(self):
        from metrics import WS_LISTENERS_ACTIVE, WS_LISTENER_ACTIVE
        mgr = self._make_manager()
        url1 = "https://mevx.io/?chain=restart1"
        url2 = "https://mevx.io/?chain=restart2"
        mgr.create_tab(url1)
        mgr.create_tab(url2)
        self.assertEqual(WS_LISTENERS_ACTIVE._value.get(), 2)
        mgr.stop()
        # stop() must collapse the global gauge to 0, NOT leave it inflated so a
        # later restart_browser() re-increments (drift) on top of stale values.
        self.assertEqual(WS_LISTENERS_ACTIVE._value.get(), 0)
        self.assertEqual(WS_LISTENER_ACTIVE.labels(url=url1)._value.get(), 0)
        # Recreate primaries the way restart_browser() does after stop()+start().
        mgr._loop = asyncio.new_event_loop()
        mgr._call = lambda coro, *a, **k: mgr._loop.run_until_complete(coro)
        fake_browser = mock.Mock()
        fake_browser.get = mock.AsyncMock(return_value=_FakeTab())
        mgr._browser = fake_browser
        mgr.create_tab(url1)
        self.assertEqual(WS_LISTENERS_ACTIVE._value.get(), 1)
        self.assertEqual(WS_LISTENER_ACTIVE.labels(url=url1)._value.get(), 1)
        self.assertEqual(WS_LISTENER_ACTIVE.labels(url=url2)._value.get(), 0)

    def test_swap_primary_returns_cas_bool(self):
        mgr = self._make_manager()
        url = "https://mevx.io/?chain=cas"
        primary = mgr.create_tab(url)
        shadow = mgr.create_tab(url)
        # url points at primary -> CAS succeeds -> True
        self.assertTrue(mgr.swap_primary(url, primary.tab_id, shadow.tab_id))
        # url now points at shadow -> old_tab_id no longer current -> False
        self.assertFalse(mgr.swap_primary(url, primary.tab_id, shadow.tab_id))
        # non-existent url -> False
        self.assertFalse(mgr.swap_primary("https://nope.io", "x", "y"))

    def test_schedule_recycling_submits_and_stop_shuts_down_executor(self):
        mgr = self._make_manager()
        self.assertIsNotNone(mgr._recycle_executor)
        ran = []
        def task():
            ran.append(True)
        fut = mgr.schedule_recycling("https://x.io", "test", task)
        self.assertEqual(fut.result(timeout=5), None)
        self.assertEqual(ran, [True])
        mgr.stop()
        # stop() shuts the executor down (wait=False) and nulls it.
        self.assertIsNone(mgr._recycle_executor)

    def test_start_recreates_executor_after_stop(self):
        mgr = self._make_manager()
        mgr.stop()
        self.assertIsNone(mgr._recycle_executor)
        # schedule_recycling (the seam the maintenance loop uses) must resurrect
        # a nulled executor rather than raising, so recycles work after a
        # restart_browser() internal stop()/start() cycle.
        ran = []
        fut = mgr.schedule_recycling("https://x.io", "test", lambda: ran.append(1))
        self.assertEqual(fut.result(timeout=5), None)
        self.assertEqual(ran, [1])
        self.assertIsNotNone(mgr._recycle_executor)

    def test_tab_state_frame_classification_and_data_frame_rate(self):
        tab = TabState(
            tab_id="tab_1",
            url="https://mevx.io/?chain=solana",
            tab=_FakeTab(),
            target_id="t1",
        )
        self.assertEqual(tab.last_data_frame_ts, 0.0)
        self.assertEqual(tab.last_control_frame_ts, 0.0)
        self.assertEqual(len(tab.data_frame_history), 0)
        self.assertEqual(tab.data_frame_rate(60.0), 0.0)

        # 1. Feed data frame (MevX jsonrpc)
        mevx_data_payload = '{"jsonrpc": "2.0", "id": 1, "result": {"slot": 100}}'
        tab._handle_frame("webSocketFrameReceived", mevx_data_payload)
        self.assertGreater(tab.last_data_frame_ts, 0.0)
        self.assertEqual(tab.last_control_frame_ts, 0.0)
        self.assertEqual(len(tab.data_frame_history), 1)

        # 2. Feed data frame (MevX subscribeFlashPool)
        mevx_flash_payload = '{"method": "subscribeFlashPool", "params": {"poolAddress": "0xabc", "createdAt": "2026-08-29"}}'
        tab._feed("webSocketFrameReceived", mevx_flash_payload)
        self.assertEqual(len(tab.data_frame_history), 2)

        # 3. Feed control frame (GMGN heartbeat)
        gmgn_heartbeat_payload = '{"action": "heartbeat"}'
        tab._handle_frame("webSocketFrameReceived", gmgn_heartbeat_payload)
        self.assertGreater(tab.last_control_frame_ts, 0.0)
        # data_frame_history should NOT increment on control frames
        self.assertEqual(len(tab.data_frame_history), 2)

        # 4. Feed ping string control frame
        tab._feed("webSocketFrameReceived", "ping")
        self.assertEqual(len(tab.data_frame_history), 2)

        # 5. Check data frame rate over 60s window
        rate = tab.data_frame_rate(window_s=60.0)
        self.assertAlmostEqual(rate, 2.0 / 60.0, places=4)

        # 6. Check data frame rate with window_s <= 0
        self.assertEqual(tab.data_frame_rate(window_s=0.0), 0.0)
        self.assertEqual(tab.data_frame_rate(window_s=-5.0), 0.0)

        # 7. Check expired timestamps in history are excluded from rolling calculation
        tab.data_frame_history.clear()
        tab.data_frame_history.append(time.time() - 120.0)  # 2 minutes ago
        self.assertEqual(tab.data_frame_rate(window_s=60.0), 0.0)

    def test_soft_reload_tab_success(self):
        mgr = self._make_manager()
        tab = mgr.create_tab("https://mevx.io/?chain=solana")
        tab.consecutive_stalls = 3

        ok = mgr.soft_reload_tab(tab.tab_id)
        self.assertTrue(ok)
        self.assertEqual(tab.status, "running")
        self.assertEqual(tab.consecutive_stalls, 0)

    def test_soft_reload_tab_failure_sets_status_crashed(self):
        mgr = self._make_manager()
        tab = mgr.create_tab("https://mevx.io/?chain=solana")
        tab.tab.get = mock.AsyncMock(side_effect=RuntimeError("Navigation timeout"))

        ok = mgr.soft_reload_tab(tab.tab_id)
        self.assertFalse(ok)
        self.assertEqual(tab.status, "crashed")

    def test_soft_reload_tab_nonexistent_id_returns_false(self):
        mgr = self._make_manager()
        ok = mgr.soft_reload_tab("nonexistent_id")
        self.assertFalse(ok)

    def test_launch_browser_applies_v8_heap_flag(self):
        captured = {}
        fake_browser = mock.Mock()
        async def fake_start(**kwargs):
            captured.update(kwargs)
            return fake_browser
        with mock.patch("chrome_manager.uc.start", side_effect=fake_start):
            with mock.patch.dict('os.environ', {'WS_CHROME_V8_HEAP_MB': '768'}, clear=False):
                browser = asyncio.run(chrome_manager._launch_browser())
                self.assertIs(browser, fake_browser)
        args = captured["browser_args"]
        self.assertIn("--js-flags=--max-old-space-size=768", args)

    def test_cdp_detached_and_target_crashed_handlers_set_status_crashed(self):
        import nodriver as uc
        mgr = self._make_manager()
        tab = mgr.create_tab("https://mevx.io/?chain=solana")
        self.assertEqual(tab.status, "running")

        # Find registered Detached and TargetCrashed handlers
        detached_handler = None
        target_crashed_handler = None
        for h in tab.handlers:
            name = getattr(h, "__name__", "")
            if name == "on_detached":
                detached_handler = h
            elif name == "on_target_crashed":
                target_crashed_handler = h

        self.assertIsNotNone(detached_handler)
        self.assertIsNotNone(target_crashed_handler)

        # Trigger Detached handler
        fake_detached_event = mock.Mock()
        mgr._loop.run_until_complete(detached_handler(fake_detached_event))
        self.assertEqual(tab.status, "crashed")

        # Reset and trigger TargetCrashed handler
        tab.status = "running"
        fake_crashed_event = mock.Mock()
        mgr._loop.run_until_complete(target_crashed_handler(fake_crashed_event))
        self.assertEqual(tab.status, "crashed")

    def test_standby_browser_launch(self):
        import nodriver as uc
        import flaresolverr_service as fs
        mgr = self._make_manager()

        fake_standby_browser = mock.Mock()
        fake_standby_browser.stop = mock.Mock()
        fake_standby_browser.get = mock.AsyncMock(return_value=_FakeTab())

        with mock.patch("nodriver.start", mock.AsyncMock(return_value=fake_standby_browser)) as m_start:
            standby = mgr.launch_standby_browser()
            self.assertIs(standby, fake_standby_browser)
            self.assertTrue(m_start.called)
            # Verify custom flags used for standby browser footprint
            call_kwargs = m_start.call_args.kwargs
            self.assertEqual(call_kwargs.get("port"), 0)
            args = call_kwargs.get("browser_args", [])
            self.assertIn("--blink-settings=imagesEnabled=false", args)
            self.assertIn("--window-size=800,600", args)
            self.assertIn("--js-flags=--max-old-space-size=256", args)
            self.assertTrue(call_kwargs.get("user_data_dir").startswith("/tmp/tabmgr_standby_") or "tabmgr_standby_" in call_kwargs.get("user_data_dir"))

            # Verify registered with orphan sweeper
            with fs._shared_dirs_lock:
                self.assertIn(standby.user_data_dir, fs._shared_browser_dirs)

    def test_standby_warm_tabs_staggered_concurrency(self):
        mgr = self._make_manager()
        fake_standby_browser = mock.Mock()
        fake_standby_browser.get = mock.AsyncMock(return_value=_FakeTab())

        urls = [
            "https://mevx.io/?chain=solana",
            "https://mevx.io/?chain=bsc",
            "https://gmgn.ai/sol",
        ]
        warmed = mgr.warm_standby_tabs(fake_standby_browser, urls, concurrency=2, timeout=0.1)
        self.assertEqual(len(warmed), 3)
        for u in urls:
            self.assertIn(u, warmed)
            self.assertEqual(warmed[u].status, "running")
            self.assertEqual(warmed[u].url, u)

    def test_standby_swap_browser_and_cleanup(self):
        from frame_router import FrameRouter
        import metrics as m
        import flaresolverr_service as fs

        mgr = self._make_manager(max_tabs=5)
        router = FrameRouter(mgr)

        url1 = "https://mevx.io/?chain=solana"
        url2 = "https://gmgn.ai/sol"

        # 1. Establish initial primary tabs on old browser
        old_tab1 = mgr.create_tab(url1)
        old_tab2 = mgr.create_tab(url2)

        old_dir = "/tmp/tabmgr_old_standby_test"
        fs.register_shared_browser_dir(old_dir)
        mgr._shared_browser_user_data_dir = old_dir

        old_browser_stop_called = []
        mgr._browser.stop = lambda: old_browser_stop_called.append(True)

        # Feed old frames
        now = time.time()
        old_tab1.frame_buffer.append({"timestamp": now, "type": "webSocketFrameReceived", "url": url1, "payload": "old_msg_1", "cdp_ts": 100.0})
        old_tab2.frame_buffer.append({"timestamp": now, "type": "webSocketFrameReceived", "url": url2, "payload": "old_msg_2", "cdp_ts": 100.0})

        # 2. Create standby browser & warmed tabs
        standby_browser = mock.Mock()
        standby_browser.stop = mock.Mock()
        standby_dir = "/tmp/tabmgr_standby_new"
        fs.register_shared_browser_dir(standby_dir)

        new_tab1 = TabState(tab_id="standby_t1", url=url1, tab=_FakeTab(), target_id="st1", status="running")
        new_tab2 = TabState(tab_id="standby_t2", url=url2, tab=_FakeTab(), target_id="st2", status="running")
        # Standby tab receives a overlapping duplicate and a new message
        new_tab1.frame_buffer.append({"timestamp": now + 0.1, "type": "webSocketFrameReceived", "url": url1, "payload": "old_msg_1", "cdp_ts": 1.0})
        new_tab1.frame_buffer.append({"timestamp": now + 0.5, "type": "webSocketFrameReceived", "url": url1, "payload": "new_msg_standby", "cdp_ts": 1.5})

        warmed_tabs = {url1: new_tab1, url2: new_tab2}

        # 3. Swap standby browser
        mgr.swap_standby_browser(standby_browser, standby_dir, warmed_tabs, router, quiescence_s=0.0)

        # 4. Verify primary swapped via CAS
        self.assertEqual(mgr._url_index[url1], "standby_t1")
        self.assertEqual(mgr._url_index[url2], "standby_t2")

        # 5. Verify buffer merge and cross-process deduplication
        drained1 = mgr.drain_tab(url1)
        payloads1 = [f["payload"] for f in drained1]
        self.assertEqual(payloads1, ["old_msg_1", "new_msg_standby"])

        # 6. Verify old tabs removed from _tabs registry
        self.assertNotIn(old_tab1.tab_id, mgr._tabs)
        self.assertNotIn(old_tab2.tab_id, mgr._tabs)
        self.assertIn("standby_t1", mgr._tabs)
        self.assertIn("standby_t2", mgr._tabs)

        # 7. Verify old browser stopped and old user_data_dir unregistered
        self.assertEqual(old_browser_stop_called, [True])
        with fs._shared_dirs_lock:
            self.assertNotIn(old_dir, fs._shared_browser_dirs)
            self.assertIn(standby_dir, fs._shared_browser_dirs)

        # 8. Verify browser promoted
        self.assertIs(mgr._browser, standby_browser)
        self.assertEqual(mgr._shared_browser_user_data_dir, standby_dir)
