import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock
from unittest.mock import MagicMock, patch

import utils
import flaresolverr_service
import flaresolverr


class TestKillChromeByUserDataDir(unittest.TestCase):

    def test_kills_and_removes(self):
        with mock.patch("utils.subprocess.run") as run, \
                mock.patch("utils.shutil.rmtree") as rmtree:
            utils._kill_chrome_by_user_data_dir("/tmp/flaresolverr_abc")
            run.assert_called_once_with(
                ["pkill", "-f", "--user-data-dir=/tmp/flaresolverr_abc"],
                check=False, timeout=10,
            )
            rmtree.assert_called_once_with("/tmp/flaresolverr_abc", ignore_errors=True)

    def test_never_raises(self):
        with mock.patch("utils.subprocess.run", side_effect=Exception("boom")), \
                mock.patch("utils.shutil.rmtree", side_effect=Exception("boom")):
            utils._kill_chrome_by_user_data_dir("/tmp/flaresolverr_abc")


class TestOrphanProcessDirs(unittest.TestCase):

    def _fake_proc(self, files):
        # files: {pid: cmdline_bytes}
        orig_listdir = os.listdir
        orig_open = open

        def fake_listdir(path):
            if path == "/proc":
                return list(files.keys())
            return orig_listdir(path)

        def fake_open(path, *args, **kwargs):
            if isinstance(path, str) and path.startswith("/proc/"):
                pid = path.split("/")[2]
                if path.endswith("cmdline"):
                    return mock.mock_open(read_data=files[pid])()
            return orig_open(path, *args, **kwargs)

        return fake_listdir, fake_open

    def test_parses_user_data_dir_and_skips_non_chrome(self):
        files = {
            "100": b"chrome\x00--user-data-dir=/tmp/flaresolverr_live\x00",
            "101": b"bash\x00--user-data-dir=/tmp/flaresolverr_other\x00",
        }
        with mock.patch("utils._get_boot_time", return_value=1000.0), \
                mock.patch("os.listdir", side_effect=self._fake_proc(files)[0]), \
                mock.patch("builtins.open", side_effect=self._fake_proc(files)[1]):
            result = utils._orphan_process_dirs()
        self.assertIn("/tmp/flaresolverr_live", result)
        self.assertNotIn("/tmp/flaresolverr_other", result)


class TestKillOrphanedChrome(unittest.TestCase):

    def test_spares_live_dir(self):
        with mock.patch("utils._orphan_process_dirs", return_value={
            "/tmp/flaresolverr_live": (5000.0, [10]),
        }), mock.patch("utils.os.kill") as kill, \
                mock.patch("utils.shutil.rmtree") as rmtree:
            utils.kill_orphaned_chrome({"/tmp/flaresolverr_live"})
            kill.assert_not_called()
            rmtree.assert_not_called()

    def test_kills_stale_unknown_dir(self):
        with mock.patch("utils._orphan_process_dirs", return_value={
            "/tmp/flaresolverr_stale": (5000.0, [10, 11]),
        }), mock.patch("utils.os.kill") as kill, \
                mock.patch("utils.shutil.rmtree") as rmtree, \
                mock.patch("utils.time.sleep") as sleep:
            utils.kill_orphaned_chrome(set())
            self.assertEqual(
                [mock.call(10, signal.SIGTERM), mock.call(11, signal.SIGTERM)],
                kill.call_args_list[:2],
            )
            sleep.assert_called_once_with(2)
            rmtree.assert_called_once_with("/tmp/flaresolverr_stale", ignore_errors=True)

    def test_skips_young_unknown_dir(self):
        with mock.patch("utils._orphan_process_dirs", return_value={
            "/tmp/flaresolverr_young": (5.0, [10]),
        }), mock.patch("utils.os.kill") as kill, \
                mock.patch("utils.shutil.rmtree") as rmtree:
            utils.kill_orphaned_chrome(set())
            kill.assert_not_called()
            rmtree.assert_not_called()

    def test_never_raises(self):
        with mock.patch("utils._orphan_process_dirs", side_effect=Exception("boom")):
            utils.kill_orphaned_chrome(set())


class TestSweepStaleProfileDirs(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.old = os.path.join(self.tmp, 'flaresolverr_old')
        self.young = os.path.join(self.tmp, 'flaresolverr_young')
        for d in (self.old, self.young):
            os.mkdir(d)
        old_t = time.time() - 3600
        os.utime(self.old, (old_t, old_t))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_removes_old_nonlive_dirs(self):
        n = utils.sweep_stale_profile_dirs(set(), max_age_seconds=600, scan_dir=self.tmp)
        self.assertGreaterEqual(n, 1)
        self.assertFalse(os.path.exists(self.old))

    def test_skips_young_dirs(self):
        utils.sweep_stale_profile_dirs(set(), max_age_seconds=600, scan_dir=self.tmp)
        self.assertTrue(os.path.exists(self.young))

    def test_skips_live_dirs_even_old(self):
        live = os.path.join(self.tmp, 'flaresolverr_live')
        os.mkdir(live)
        old_t = time.time() - 3600
        os.utime(live, (old_t, old_t))
        utils.sweep_stale_profile_dirs({live}, max_age_seconds=600, scan_dir=self.tmp)
        self.assertTrue(os.path.exists(live))

    def test_skips_dirs_with_running_chrome(self):
        procdir = os.path.join(self.tmp, 'flaresolverr_procowned')
        os.mkdir(procdir)
        old_t = time.time() - 3600
        os.utime(procdir, (old_t, old_t))
        with mock.patch.object(utils, '_orphan_process_dirs',
                               return_value={procdir: [999.0, [4242]]}):
            utils.sweep_stale_profile_dirs(set(), max_age_seconds=600, scan_dir=self.tmp)
        self.assertTrue(os.path.exists(procdir))

    def test_config_getter_default_and_override(self):
        with mock.patch.dict('os.environ', {}, clear=True):
            self.assertEqual(utils.get_config_profile_dir_max_age(), 600)
        with mock.patch.dict('os.environ', {'PROFILE_DIR_MAX_AGE_S': '300'}):
            self.assertEqual(utils.get_config_profile_dir_max_age(), 300)


class TestGetWebDriverFailureReap(unittest.TestCase):

    def test_kills_profile_on_launch_failure(self):
        with mock.patch("utils.uc.Chrome", side_effect=Exception("cannot connect")), \
                mock.patch("utils.get_chrome_exe_path", return_value="/usr/bin/chrome"), \
                mock.patch("utils.get_config_headless", return_value=False), \
                mock.patch("utils._kill_chrome_by_user_data_dir") as kill, \
                mock.patch("utils.tempfile.mkdtemp", return_value="/tmp/flaresolverr_test") as mkdtemp:
            with self.assertRaises(Exception):
                utils.get_webdriver()
            mkdtemp.assert_called_once_with(prefix=utils.FS_CHROME_PROFILE_PREFIX)
            kill.assert_called_once_with("/tmp/flaresolverr_test")


def _make_mock_session(session_id="s1", user_data_dir="/tmp/flaresolverr_session"):
    driver = SimpleNamespace(
        _fs_user_data_dir=user_data_dir,
        quit=lambda: None,
        close=lambda: None,
    )
    return SimpleNamespace(
        session_id=session_id,
        driver=driver,
    )


class TestLiveUserDataDirs(unittest.TestCase):

    def setUp(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()
        flaresolverr.ws_listener_manager.listeners.clear()

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()
        flaresolverr.ws_listener_manager.listeners.clear()

    def test_collects_from_sessions_storage(self):
        session = _make_mock_session("s1", "/tmp/flaresolverr_sess1")
        flaresolverr_service.SESSIONS_STORAGE.sessions["s1"] = session

        dirs = flaresolverr_service._live_user_data_dirs()

        self.assertEqual(dirs, {"/tmp/flaresolverr_sess1"})

    def test_collects_from_ws_listeners(self):
        session = _make_mock_session("ws_listener_abc", "/tmp/flaresolverr_ws1")
        flaresolverr_service.SESSIONS_STORAGE.sessions["ws_listener_abc"] = session
        listener = flaresolverr_service.WebSocketListener(
            listener_id="abc", session_id="ws_listener_abc", url="https://x.io")
        flaresolverr.ws_listener_manager.listeners["abc"] = listener

        dirs = flaresolverr_service._live_user_data_dirs()

        self.assertEqual(dirs, {"/tmp/flaresolverr_ws1"})

    def test_collects_from_both_sources(self):
        session1 = _make_mock_session("s1", "/tmp/flaresolverr_sess1")
        session2 = _make_mock_session("ws_listener_abc", "/tmp/flaresolverr_ws1")
        flaresolverr_service.SESSIONS_STORAGE.sessions["s1"] = session1
        flaresolverr_service.SESSIONS_STORAGE.sessions["ws_listener_abc"] = session2
        listener = flaresolverr_service.WebSocketListener(
            listener_id="abc", session_id="ws_listener_abc", url="https://x.io")
        flaresolverr.ws_listener_manager.listeners["abc"] = listener

        dirs = flaresolverr_service._live_user_data_dirs()

        self.assertEqual(dirs, {"/tmp/flaresolverr_sess1", "/tmp/flaresolverr_ws1"})

    def test_skips_sessions_without_driver_dir(self):
        session = SimpleNamespace(
            session_id="s1",
            driver=SimpleNamespace(quit=lambda: None),
        )
        flaresolverr_service.SESSIONS_STORAGE.sessions["s1"] = session

        dirs = flaresolverr_service._live_user_data_dirs()

        self.assertEqual(dirs, set())

    def test_skips_listeners_without_session_id(self):
        listener = flaresolverr_service.WebSocketListener(
            listener_id="abc", session_id="", url="https://x.io")
        flaresolverr.ws_listener_manager.listeners["abc"] = listener

        dirs = flaresolverr_service._live_user_data_dirs()

        self.assertEqual(dirs, set())


class TestRequestPathFailureCallsKillOrphaned(unittest.TestCase):

    def test_calls_kill_orphaned_on_get_webdriver_failure(self):
        with mock.patch("utils.get_webdriver", side_effect=Exception("launch failed")), \
             mock.patch("utils.kill_orphaned_chrome") as kill_mock, \
             mock.patch.object(flaresolverr_service, "_live_user_data_dirs", return_value={"/tmp/live1"}):
            req = SimpleNamespace(proxy=None, session=None, maxTimeout=60000,
                                  session_ttl_minutes=None, url="https://x.io",
                                  method="GET", headers=None, postData=None,
                                  returnRawHtml=None, download=None, disableMedia=None,
                                  cookies=None, returnOnlyCookies=None, waitInSeconds=None,
                                  returnScreenshot=None, tabs_till_verify=None)
            with self.assertRaises(Exception) as cm:
                flaresolverr_service._resolve_challenge(req, "GET")
            kill_mock.assert_called_once_with({"/tmp/live1"})
            self.assertIn("Error solving the challenge", str(cm.exception))


class TestListenerFailureCallsKillOrphaned(unittest.TestCase):

    def setUp(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()
        flaresolverr.ws_listener_manager.listeners.clear()
        self._orig_create = flaresolverr_service.SESSIONS_STORAGE.create
        self._orig_destroy = flaresolverr_service.SESSIONS_STORAGE.destroy

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.create = self._orig_create
        flaresolverr_service.SESSIONS_STORAGE.destroy = self._orig_destroy
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()
        flaresolverr.ws_listener_manager.listeners.clear()

    def test_calls_kill_orphaned_on_create_session_failure(self):
        with mock.patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                              side_effect=Exception("create failed")), \
             mock.patch("utils.kill_orphaned_chrome") as kill_mock, \
             mock.patch.object(flaresolverr_service, "_live_user_data_dirs", return_value={"/tmp/live2"}):

            manager = flaresolverr_service.WebSocketListenerManager(max_listeners=2)
            with self.assertRaises(Exception):
                manager.create_listener("https://x.io")

            kill_mock.assert_called_once_with({"/tmp/live2"})

    def test_calls_kill_orphaned_on_driver_get_failure(self):
        session = _make_mock_session("ws_listener_abc", "/tmp/flaresolverr_ws1")
        with mock.patch.object(flaresolverr_service.SESSIONS_STORAGE, "create",
                              return_value=(session, True)), \
             mock.patch.object(flaresolverr_service.SESSIONS_STORAGE, "destroy") as mock_destroy, \
             mock.patch("utils.kill_orphaned_chrome") as kill_mock, \
             mock.patch.object(flaresolverr_service, "_live_user_data_dirs", return_value={"/tmp/live3"}), \
             mock.patch("flaresolverr_service.func_timeout",
                        side_effect=Exception("timeout")):

            manager = flaresolverr_service.WebSocketListenerManager(max_listeners=2)
            with self.assertRaises(Exception):
                manager.create_listener("https://x.io")

            kill_mock.assert_called_once_with({"/tmp/live3"})
            mock_destroy.assert_called_once()


class TestBackgroundTasksThreadWatchdog(unittest.TestCase):

    def setUp(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()
        flaresolverr.ws_listener_manager.listeners.clear()

    def tearDown(self):
        flaresolverr_service.SESSIONS_STORAGE.sessions.clear()
        flaresolverr.ws_listener_manager.listeners.clear()

    def test_watchdog_calls_kill_orphaned_with_live_dirs(self):
        with mock.patch("utils.kill_orphaned_chrome") as kill_mock, \
             mock.patch("utils.sweep_stale_profile_dirs"), \
             mock.patch.object(flaresolverr_service, "_live_user_data_dirs", return_value={"/tmp/live_watchdog"}) as live_mock, \
             mock.patch("flaresolverr.flaresolverr_service.SESSIONS_STORAGE.cleanup_stale_sessions"), \
             mock.patch("flaresolverr.ws_listener_manager.cleanup_stale"), \
             mock.patch("flaresolverr.time.sleep", side_effect=KeyboardInterrupt):

            try:
                flaresolverr.background_tasks_thread()
            except KeyboardInterrupt:
                pass

            live_mock.assert_called_once()
            kill_mock.assert_called_once_with({"/tmp/live_watchdog"})


class TestDriverTimeoutHardening(unittest.TestCase):

    def test_config_getters_defaults_and_overrides(self):
        with mock.patch.dict('os.environ', {}, clear=True):
            self.assertEqual(utils.get_config_driver_command_timeout(), 120)
            self.assertEqual(utils.get_config_page_load_timeout(), 75)
            self.assertEqual(utils.get_config_shutdown_grace(), 10)
        with mock.patch.dict('os.environ', {'DRIVER_COMMAND_TIMEOUT': '60',
                                            'PAGE_LOAD_TIMEOUT': '30',
                                            'SHUTDOWN_GRACE': '5'}):
            self.assertEqual(utils.get_config_driver_command_timeout(), 60)
            self.assertEqual(utils.get_config_page_load_timeout(), 30)
            self.assertEqual(utils.get_config_shutdown_grace(), 5)

    def test_harden_driver_timeouts_sets_all_layers(self):
        driver = mock.MagicMock()
        cc = mock.MagicMock()
        driver.command_executor = mock.MagicMock()
        driver.command_executor._client_config = cc
        utils._harden_driver_timeouts(driver)
        self.assertEqual(cc.timeout, 120)
        driver.set_page_load_timeout.assert_called_once_with(75)
        driver.set_script_timeout.assert_called_once_with(30)

    def test_harden_driver_falls_back_without_client_config(self):
        executor = SimpleNamespace(set_timeout=mock.Mock())
        driver = SimpleNamespace(command_executor=executor,
                                 set_page_load_timeout=mock.Mock(),
                                 set_script_timeout=mock.Mock())
        utils._harden_driver_timeouts(driver)
        executor.set_timeout.assert_called_once_with(120)
        driver.set_page_load_timeout.assert_called_once_with(75)


class TestSafeQuit(unittest.TestCase):

    def _mk_driver(self, quit_behavior=None):
        driver = MagicMock()
        driver.service.process.pid = 1111
        driver.browser_pid = 2222
        driver._fs_user_data_dir = '/tmp/fs_test_profile'
        driver.service.send_remote_shutdown_command = MagicMock()
        if quit_behavior:
            driver.quit.side_effect = quit_behavior
        return driver

    def test_safe_quit_normal_path_no_escalation(self):
        driver = self._mk_driver()
        with patch.object(utils, '_escalate_kill') as esc, \
             patch.object(utils.time, 'sleep'):
            utils.safe_quit(driver)
        driver.quit.assert_called_once()
        esc.assert_not_called()

    def test_safe_quit_escalates_on_hung_quit(self):
        def hang():
            time.sleep(30)
        driver = self._mk_driver(quit_behavior=hang)
        with patch.object(utils, '_escalate_kill') as esc, \
             patch.dict('os.environ', {'SHUTDOWN_GRACE': '1'}):
            start = time.monotonic()
            utils.safe_quit(driver)
            elapsed = time.monotonic() - start
        self.assertLess(elapsed, 8)           # grace + 2*grace worst case
        esc.assert_called_once_with(driver, '/tmp/fs_test_profile')

    def test_bounded_shutdown_thread_abandoned_after_grace(self):
        driver = self._mk_driver()
        release = threading.Event()
        slow = MagicMock(side_effect=lambda: release.wait(30))
        driver.service.send_remote_shutdown_command = slow
        with patch.object(utils, '_escalate_kill'), \
             patch.dict('os.environ', {'SHUTDOWN_GRACE': '1'}):
            utils.safe_quit(driver)
        slow.assert_called_once()             # ran bounded, not forever

    def test_escalation_not_armed_during_shutdown_phase(self):
        driver = self._mk_driver()
        order = []
        entered = threading.Event()

        class TrackingTimer(threading.Timer):
            def start(self):
                order.append('timer_started')
                super().start()

        def slow_shutdown():
            entered.set()
            time.sleep(30)

        driver.service.send_remote_shutdown_command = MagicMock(side_effect=slow_shutdown)
        with patch.object(utils.threading, 'Timer', TrackingTimer), \
             patch.object(utils, '_escalate_kill'), \
             patch.dict('os.environ', {'SHUTDOWN_GRACE': '5'}):
            t = threading.Thread(target=utils.safe_quit, args=(driver,), daemon=True)
            t.start()
            self.assertTrue(entered.wait(5))
            time.sleep(0.3)                    # mid-shutdown-phase sample
            self.assertNotIn('timer_started', order)
            t.join(15)
        self.assertIn('timer_started', order)  # armed once quit phase began

    def test_escalate_kill_terms_then_kills_and_sweeps(self):
        driver = self._mk_driver()
        with patch.object(utils.os, 'kill') as m_kill, \
             patch.object(utils, '_kill_chrome_by_user_data_dir') as m_sweep, \
             patch.object(utils.time, 'sleep'):
            utils._escalate_kill(driver, '/tmp/fs_test_profile')
        kill_calls = [c.args[0] for c in m_kill.call_args_list]
        self.assertIn(1111, kill_calls)
        self.assertIn(2222, kill_calls)
        m_sweep.assert_called_once_with('/tmp/fs_test_profile')


if __name__ == "__main__":
    unittest.main()
