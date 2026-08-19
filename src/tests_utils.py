import os
import shutil
import signal
import subprocess
import tempfile
import unittest
from unittest import mock

import utils


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


if __name__ == "__main__":
    unittest.main()
