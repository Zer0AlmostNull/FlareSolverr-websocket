import asyncio
import threading
import time
import unittest
from unittest import mock
from collections import deque

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
