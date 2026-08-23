import json
import logging
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

from selenium.webdriver.chrome.webdriver import WebDriver
import undetected_chromedriver as uc

FLARESOLVERR_VERSION = None
PLATFORM_VERSION = None
CHROME_EXE_PATH = None
CHROME_MAJOR_VERSION = None
USER_AGENT = None
XVFB_DISPLAY = None
PATCHED_DRIVER_PATH = None

FS_CHROME_PROFILE_PREFIX = "flaresolverr_"


def _get_boot_time() -> float:
    try:
        import psutil
        return psutil.boot_time()
    except Exception:
        pass
    try:
        with open("/proc/stat") as f:
            for line in f:
                if line.startswith("btime"):
                    return float(line.strip().split()[1])
    except Exception:
        pass
    return 0.0


def _kill_chrome_by_user_data_dir(user_data_dir: str) -> None:
    """Kill Chrome processes using the specific user-data-dir."""
    try:
        orphan_dirs = _orphan_process_dirs()
        if user_data_dir in orphan_dirs:
            age, pids = orphan_dirs[user_data_dir]
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
            time.sleep(2)
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except Exception:
                    pass
        shutil.rmtree(user_data_dir, ignore_errors=True)
    except Exception:
        pass


def _escalate_kill(driver, user_data_dir):
    """Best-effort forceful teardown for a driver whose quit() hung:
    TERM->KILL chromedriver + browser pids, then sweep the profile dir
    (reuses the orphan-reaper's TERM/KILL/rmtree machinery)."""
    service = getattr(driver, 'service', None)
    proc = getattr(service, 'process', None)
    pids = [getattr(proc, 'pid', None), getattr(driver, 'browser_pid', None)]
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for pid in pids:
            if isinstance(pid, int) and pid > 0:
                try:
                    os.kill(pid, sig)
                except Exception:
                    pass
        if sig == signal.SIGTERM:
            time.sleep(2)
    if user_data_dir:
        try:
            _kill_chrome_by_user_data_dir(user_data_dir)
        except Exception:
            logging.debug("profile sweep after escalation failed", exc_info=True)


def safe_quit(driver, grace=None):
    """driver.quit() that cannot hang forever. UC's quit() does an un-timeouted
    urlopen(/shutdown) against chromedriver before killing it; a hung-but-alive
    chromedriver therefore wedged destroy() indefinitely, leaking the entire
    process tree. Here: (a) the chromedriver shutdown-command is run on a
    daemon thread we abandon after `grace` seconds; (b) driver.quit() runs on
    a daemon thread abandoned after grace*2; (c) an escalation timer
    force-kills pids and sweeps the profile if quit() hasn't returned within
    grace*2."""
    grace = grace or get_config_shutdown_grace()
    service = getattr(driver, 'service', None)
    orig_shutdown = getattr(service, 'send_remote_shutdown_command', None)

    def bounded_shutdown():
        t = threading.Thread(target=orig_shutdown, daemon=True)
        t.start()
        t.join(grace)

    if callable(orig_shutdown):
        try:
            service.send_remote_shutdown_command = bounded_shutdown
        except Exception:
            logging.debug("could not bound send_remote_shutdown_command", exc_info=True)
    udd = getattr(driver, '_fs_user_data_dir', None)
    killer = threading.Timer(grace * 2, _escalate_kill, args=(driver, udd))
    killer.daemon = True
    killer.start()
    outcome = {}

    def do_quit():
        try:
            driver.quit()
        except BaseException as e:
            outcome['error'] = e

    try:
        if callable(orig_shutdown):
            try:
                bounded_shutdown()
            except Exception:
                logging.debug("bounded shutdown-command failed", exc_info=True)
        quitter = threading.Thread(target=do_quit, daemon=True)
        quitter.start()
        quitter.join(grace * 2)
        if 'error' in outcome:
            raise outcome['error']
    finally:
        killer.cancel()


def _harden_driver_timeouts(driver):
    """Bound every layer of selenium blocking so a hung chromedriver raises
    instead of freezing a thread forever. Order matters: the client-config
    timeout is set FIRST because it bounds the page-load-timeout HTTP call
    itself."""
    # 1) Bound EVERY selenium HTTP command (urllib3 recv otherwise blocks forever)
    cmd_timeout = get_config_driver_command_timeout()
    executor = getattr(driver, 'command_executor', None)
    client_config = getattr(executor, '_client_config', None)
    if client_config is not None:
        client_config.timeout = cmd_timeout
    elif executor is not None and hasattr(executor, 'set_timeout'):
        executor.set_timeout(cmd_timeout)
    # 2) Make driver.get() self-bounding inside chrome/chromedriver
    try:
        driver.set_page_load_timeout(get_config_page_load_timeout())
    except Exception as e:
        logging.warning(f"set_page_load_timeout failed at launch: {e}")
    try:
        driver.set_script_timeout(30)
    except Exception as e:
        logging.warning(f"set_script_timeout failed at launch: {e}")


def _orphan_process_dirs() -> dict:
    """Return mapping user_data_dir -> (age_seconds, [pids]) for chrome procs."""
    result = {}
    try:
        pids = [p for p in os.listdir("/proc") if p.isdigit()]
    except Exception:
        return result
    clk_tck = os.sysconf("SC_CLK_TCK")
    boot_time = _get_boot_time()
    for pid in pids:
        try:
            with open(os.path.join("/proc", pid, "cmdline"), "rb") as f:
                data = f.read().decode("utf-8", errors="ignore")
        except Exception:
            continue
        if "chrome" not in data and "chromium" not in data:
            continue
        args = [a for a in data.split("\x00") if a]
        user_data_dir = None
        for arg in args:
            if arg.startswith("--user-data-dir="):
                user_data_dir = arg.split("=", 1)[1]
                break
        if user_data_dir is None:
            continue
        try:
            pid_int = int(pid)
        except Exception:
            continue
        entries = result.setdefault(user_data_dir, [0.0, []])
        entries[1].append(pid_int)
        try:
            with open(os.path.join("/proc", pid, "stat")) as f:
                stat = f.read()
            starttime_ticks = int(stat.split(")")[1].split()[19])
            proc_boot_time = boot_time + starttime_ticks / clk_tck
            entries[0] = max(entries[0], time.time() - proc_boot_time)
        except Exception:
            pass
    return result


def kill_orphaned_chrome(live_user_data_dirs: set, grace_seconds: int = 120) -> None:
    try:
        orphan_dirs = _orphan_process_dirs()
    except Exception:
        return
    for user_data_dir, (age, pids) in orphan_dirs.items():
        if user_data_dir in live_user_data_dirs:
            continue
        if age < grace_seconds:
            continue
        try:
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
            if pids:
                time.sleep(2)
                for pid in pids:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            shutil.rmtree(user_data_dir, ignore_errors=True)
        except Exception:
            pass


def get_config_log_html() -> bool:
    return os.environ.get('LOG_HTML', 'false').lower() == 'true'


def get_config_headless() -> bool:
    return os.environ.get('HEADLESS', 'true').lower() == 'true'


def get_config_disable_media() -> bool:
    return os.environ.get('DISABLE_MEDIA', 'false').lower() == 'true'


def get_config_websocket_max_messages() -> int:
    return int(os.environ.get('WEBSOCKET_MAX_MESSAGES', '100'))


def get_config_max_ws_listeners() -> int:
    return int(os.environ.get('MAX_WS_LISTENERS', '5'))


def get_config_ws_listener_default_ttl() -> int:
    return int(os.environ.get('WS_LISTENER_DEFAULT_TTL', '30'))


def get_config_ws_listener_default_max_msgs() -> int:
    return int(os.environ.get('WS_LISTENER_DEFAULT_MAX_MSGS', '500'))


def get_config_ws_listener_create_timeout() -> int:
    return int(os.environ.get('WS_LISTENER_CREATE_TIMEOUT', '90'))


def get_config_ws_listener_max_lifetime() -> int:
    return int(os.environ.get('WS_LISTENER_MAX_LIFETIME_MINUTES', '180'))


def get_config_driver_command_timeout() -> int:
    return int(os.environ.get('DRIVER_COMMAND_TIMEOUT', '120'))


def get_config_page_load_timeout() -> int:
    return int(os.environ.get('PAGE_LOAD_TIMEOUT', '75'))


def get_config_shutdown_grace() -> int:
    return int(os.environ.get('SHUTDOWN_GRACE', '10'))


def get_config_ws_chrome_v8_heap_mb() -> int:
    return int(os.environ.get('WS_CHROME_V8_HEAP_MB', '1024'))


def get_flaresolverr_version() -> str:
    global FLARESOLVERR_VERSION
    if FLARESOLVERR_VERSION is not None:
        return FLARESOLVERR_VERSION

    package_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, 'package.json')
    if not os.path.isfile(package_path):
        package_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'package.json')
    with open(package_path) as f:
        FLARESOLVERR_VERSION = json.loads(f.read())['version']
        return FLARESOLVERR_VERSION

def get_current_platform() -> str:
    global PLATFORM_VERSION
    if PLATFORM_VERSION is not None:
        return PLATFORM_VERSION
    PLATFORM_VERSION = os.name
    return PLATFORM_VERSION


def create_proxy_extension(proxy: dict) -> str:
    parsed_url = urllib.parse.urlparse(proxy['url'])
    scheme = parsed_url.scheme
    host = parsed_url.hostname
    port = parsed_url.port
    username = proxy['username']
    password = proxy['password']
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 3,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "storage",
            "webRequest",
            "webRequestAuthProvider"
        ],
        "host_permissions": [
          "<all_urls>"
        ],
        "background": {
          "service_worker": "background.js"
        },
        "minimum_chrome_version": "76.0.0"
    }
    """

    background_js = """
    var config = {
        mode: "fixed_servers",
        rules: {
            singleProxy: {
                scheme: "%s",
                host: "%s",
                port: %d
            },
            bypassList: ["localhost"]
        }
    };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
        callbackFn,
        { urls: ["<all_urls>"] },
        ['blocking']
    );
    """ % (
        scheme,
        host,
        port,
        username,
        password
    )

    proxy_extension_dir = tempfile.mkdtemp()

    with open(os.path.join(proxy_extension_dir, "manifest.json"), "w") as f:
        f.write(manifest_json)

    with open(os.path.join(proxy_extension_dir, "background.js"), "w") as f:
        f.write(background_js)

    return proxy_extension_dir


def get_webdriver(proxy: dict = None) -> WebDriver:
    global PATCHED_DRIVER_PATH, USER_AGENT
    logging.debug('Launching web browser...')

    # Pre-launch aggressive cleanup of orphaned Chrome processes
    try:
        live_dirs = set()
        kill_orphaned_chrome(live_dirs, grace_seconds=10)
    except Exception:
        pass

    # undetected_chromedriver
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-search-engine-choice-screen')
    # todo: this param shows a warning in chrome head-full
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # this option removes the zygote sandbox (it seems that the resolution is a bit faster)
    options.add_argument('--no-zygote')
    # memory optimization
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disk-cache-size=1')
    options.add_argument('--media-cache-size=1')
    options.add_argument('--renderer-process-limit=1')

    # NEW: Stability and PID reduction flags
    options.add_argument('--headless=chrome')          # Force legacy headless (prevents --headless=new)
    options.add_argument('--disable-crash-reporter')   # Remove crashpad processes
    options.add_argument('--disable-crashpad-handler') # Remove crashpad processes
    options.add_argument('--js-flags=--max-old-space-size=%d'
                         % get_config_ws_chrome_v8_heap_mb())  # Bound V8 heap

    options.add_argument('--disable-features=IsolateOrigins,site-per-process,AudioServiceOutOfProcess')
    options.add_argument('--disable-site-isolation-trials')
    options.add_argument('--disable-v8-idle-tasks')
    options.add_argument('--process-per-site')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-component-extensions-with-background-pages')
    options.add_argument('--disable-default-apps')
    options.add_argument('--mute-audio')
    options.add_argument('--no-default-browser-check')
    options.add_argument('--autoplay-policy=no-user-gesture-required')
    options.add_argument('--disable-back-forward-cache')
    options.add_argument('--disable-background-networking')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-breakpad')
    options.add_argument('--disable-client-side-phishing-detection')
    options.add_argument('--disable-component-update')
    options.add_argument('--disable-datasaver-prompt')
    options.add_argument('--disable-domain-reliability')
    options.add_argument('--disable-hang-monitor')
    options.add_argument('--disable-ipc-flooding-protection')
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-offer-store-unmasked-wallet-cards')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-print-preview')
    options.add_argument('--disable-prompt-on-repost')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-speech-api')
    options.add_argument('--disable-sync')
    options.add_argument('--hide-scrollbars')
    options.add_argument('--ignore-gpu-blacklist')
    options.add_argument('--metrics-recording-only')
    options.add_argument('--no-first-run')
    options.add_argument('--no-pings')
    options.add_argument('--password-store=basic')
    options.add_argument('--use-mock-keychain')
    options.add_argument('--disable-gpu-sandbox')
    # attempt to fix Docker ARM32 build
    IS_ARMARCH = platform.machine().startswith(('arm', 'aarch'))
    if IS_ARMARCH:
        pass
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    # Disable the breaking Local Network Access Checks popup.
    options.add_argument('--disable-features=LocalNetworkAccessChecks')

    language = os.environ.get('LANG', None)
    if language is not None:
        options.add_argument('--accept-lang=%s' % language)

    # Fix for Chrome 117 | https://github.com/FlareSolverr/FlareSolverr/issues/910
    if USER_AGENT is not None:
        options.add_argument('--user-agent=%s' % USER_AGENT)

    proxy_extension_dir = None
    if proxy and all(key in proxy for key in ['url', 'username', 'password']):
        proxy_extension_dir = create_proxy_extension(proxy)
        options.add_argument("--disable-features=DisableLoadExtensionCommandLineSwitch")
        options.add_argument("--load-extension=%s" % os.path.abspath(proxy_extension_dir))
    elif proxy and 'url' in proxy:
        proxy_url = proxy['url']
        logging.debug("Using webdriver proxy: %s", proxy_url)
        options.add_argument('--proxy-server=%s' % proxy_url)

    # note: headless mode is detected (headless = True)
    # we launch the browser in head-full mode with the window hidden
    windows_headless = False
    if get_config_headless():
        if os.name == 'nt':
            windows_headless = True
        else:
            start_xvfb_display()
    # For normal headless mode:
    # options.add_argument('--headless')

    # if we are inside the Docker container, we avoid downloading the driver
    driver_exe_path = None
    version_main = None
    if os.path.exists("/app/chromedriver"):
        # running inside Docker
        driver_exe_path = "/app/chromedriver"
    else:
        version_main = get_chrome_major_version()
        if PATCHED_DRIVER_PATH is not None:
            driver_exe_path = PATCHED_DRIVER_PATH

    # detect chrome path
    browser_executable_path = get_chrome_exe_path()

    # give every browser a known profile dir so a failed launch can be reaped
    user_data_dir = tempfile.mkdtemp(prefix=FS_CHROME_PROFILE_PREFIX)
    options.add_argument(f"--user-data-dir={user_data_dir}")

    # downloads and patches the chromedriver
    # if we don't set driver_executable_path it downloads, patches, and deletes the driver each time
    try:
        driver = uc.Chrome(options=options, browser_executable_path=browser_executable_path,
                           driver_executable_path=driver_exe_path, version_main=version_main,
                           windows_headless=windows_headless, headless=get_config_headless(),
                           enable_cdp_events=True, user_data_dir=user_data_dir)
    except Exception as e:
        logging.error("Error starting Chrome: %s" % e)
        # reap the partially-spawned Chromium so it cannot leak
        _kill_chrome_by_user_data_dir(user_data_dir)
        # No point in continuing if we cannot retrieve the driver
        raise e

    # save the patched driver to avoid re-downloads
    if driver_exe_path is None:
        PATCHED_DRIVER_PATH = os.path.join(driver.patcher.data_path, driver.patcher.exe_name)
        if PATCHED_DRIVER_PATH != driver.patcher.executable_path:
            shutil.copy(driver.patcher.executable_path, PATCHED_DRIVER_PATH)

    driver._fs_user_data_dir = user_data_dir

    # Bound all selenium blocking layers (hang -> exception, not frozen thread)
    _harden_driver_timeouts(driver)

    # clean up proxy extension directory
    if proxy_extension_dir is not None:
        shutil.rmtree(proxy_extension_dir)

    # selenium vanilla
    # options = webdriver.ChromeOptions()
    # options.add_argument('--no-sandbox')
    # options.add_argument('--window-size=1920,1080')
    # options.add_argument('--disable-setuid-sandbox')
    # options.add_argument('--disable-dev-shm-usage')
    # driver = webdriver.Chrome(options=options)

    return driver


def get_chrome_exe_path() -> str:
    global CHROME_EXE_PATH
    if CHROME_EXE_PATH is not None:
        return CHROME_EXE_PATH
    # linux pyinstaller bundle
    chrome_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chrome', "chrome")
    if os.path.exists(chrome_path):
        if not os.access(chrome_path, os.X_OK):
            raise Exception(f'Chrome binary "{chrome_path}" is not executable. '
                            f'Please, extract the archive with "tar xzf <file.tar.gz>".')
        CHROME_EXE_PATH = chrome_path
        return CHROME_EXE_PATH
    # windows pyinstaller bundle
    chrome_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chrome', "chrome.exe")
    if os.path.exists(chrome_path):
        CHROME_EXE_PATH = chrome_path
        return CHROME_EXE_PATH
    # system
    CHROME_EXE_PATH = uc.find_chrome_executable()
    return CHROME_EXE_PATH


def get_chrome_major_version() -> str:
    global CHROME_MAJOR_VERSION
    if CHROME_MAJOR_VERSION is not None:
        return CHROME_MAJOR_VERSION

    if os.name == 'nt':
        # Example: '104.0.5112.79'
        try:
            complete_version = extract_version_nt_executable(get_chrome_exe_path())
        except Exception:
            try:
                complete_version = extract_version_nt_registry()
            except Exception:
                # Example: '104.0.5112.79'
                complete_version = extract_version_nt_folder()
    else:
        chrome_path = get_chrome_exe_path()
        process = os.popen(f'"{chrome_path}" --version')
        # Example 1: 'Chromium 104.0.5112.79 Arch Linux\n'
        # Example 2: 'Google Chrome 104.0.5112.79 Arch Linux\n'
        complete_version = process.read()
        process.close()

    CHROME_MAJOR_VERSION = complete_version.split('.')[0].split(' ')[-1]
    return CHROME_MAJOR_VERSION


def extract_version_nt_executable(exe_path: str) -> str:
    import pefile
    pe = pefile.PE(exe_path, fast_load=True)
    pe.parse_data_directories(
        directories=[pefile.DIRECTORY_ENTRY["IMAGE_DIRECTORY_ENTRY_RESOURCE"]]
    )
    return pe.FileInfo[0][0].StringTable[0].entries[b"FileVersion"].decode('utf-8')


def extract_version_nt_registry() -> str:
    stream = os.popen(
        'reg query "HKLM\\SOFTWARE\\Wow6432Node\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\Google Chrome"')
    output = stream.read()
    google_version = ''
    for letter in output[output.rindex('DisplayVersion    REG_SZ') + 24:]:
        if letter != '\n':
            google_version += letter
        else:
            break
    return google_version.strip()


def extract_version_nt_folder() -> str:
    # Check if the Chrome folder exists in the x32 or x64 Program Files folders.
    for i in range(2):
        path = 'C:\\Program Files' + (' (x86)' if i else '') + '\\Google\\Chrome\\Application'
        if os.path.isdir(path):
            paths = [f.path for f in os.scandir(path) if f.is_dir()]
            for path in paths:
                filename = os.path.basename(path)
                pattern = r'\d+\.\d+\.\d+\.\d+'
                match = re.search(pattern, filename)
                if match and match.group():
                    # Found a Chrome version.
                    return match.group(0)
    return ''


def get_user_agent(driver=None) -> str:
    global USER_AGENT
    if USER_AGENT is not None:
        return USER_AGENT

    try:
        if driver is None:
            driver = get_webdriver()
        USER_AGENT = driver.execute_script("return navigator.userAgent")
        # Fix for Chrome 117 | https://github.com/FlareSolverr/FlareSolverr/issues/910
        USER_AGENT = re.sub('HEADLESS', '', USER_AGENT, flags=re.IGNORECASE)
        return USER_AGENT
    except Exception as e:
        raise Exception("Error getting browser User-Agent. " + str(e))
    finally:
        if driver is not None:
            if PLATFORM_VERSION == "nt":
                driver.close()
            safe_quit(driver)


def start_xvfb_display():
    global XVFB_DISPLAY
    if XVFB_DISPLAY is None:
        from xvfbwrapper import Xvfb
        XVFB_DISPLAY = Xvfb()
        XVFB_DISPLAY.start()


def object_to_dict(_object):
    json_dict = json.loads(json.dumps(_object, default=lambda o: o.__dict__))
    # remove hidden fields
    return {k: v for k, v in json_dict.items() if not k.startswith('__')}
