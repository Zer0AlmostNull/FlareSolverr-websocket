# Changelog

## [2026-08-28] single-Chrome multi-tab WS listener refactor (squashed)
* Quorum-review fix wave (3 independent subagent reviewers; all Critical + Important findings fixed): `FrameRouter._merge_dedup` no longer trims unique old frames before deduping — with a full buffer and a shadow re-delivering duplicates it would evict up to N unique still-undelivered frames (reproduced −100) before the dedup dropped all N dupes, violating zero-drop; it now dedups first, then trims to the hard cap only when genuinely-new frames push past it. `get_memory_usage_gb()` now reads the REAL Chrome pid via nodriver's private `_process_pid` (the public `.pid` does not exist, so the old code measured the FlareSolverr PYTHON process RSS via `/proc/self/statm` and never fired the >1.2GB restart). The shared-Chrome wedge is now detected by an all-stale watchdog: `_tab_manager_maintenance()` restarts the whole browser ONLY when EVERY primary listener has been silent for `STALE_RESTART_WINDOW_S` (120s) with a `BROWSER_RESTART_COOLDOWN_S` (600s) guard — a single silent tab still gets the per-URL zombie recycle. `_ensure_tab_manager()` self-heals a permanently-wedged manager (whose loop died after a failed `restart_browser`) by detecting `_manager_broken` and rebuilding the singleton. `_recycle_tab`'s finally-guard now retires the shadow unless it became the url's primary (the old `status != "running"` check leaked a warmed shadow when `handoff` raised). `_boot_tab` is single-flighted against restart/recover via non-blocking `_restart_lock`. Legacy `WS_*` gauge parity: `WS_MESSAGES_TOTAL` type labels map back to `received`/`sent`, `WS_LISTENER_UPTIME` reports elapsed seconds (not a wall-clock timestamp), and `WS_LISTENERS_RUNNING`/`WS_LISTENERS_STATUS`/`WS_LISTENER_LAST_SEEN` are now maintained on the new path. `_recover()` unregisters the old shared browser dir with the orphan sweeper (previous leak: pre-crash Chrome became immortal + its profile dir leaked). `warm_tab(require_frame=...)` lets zombie recycles skip the first-frame gate (which self-defeated by aborting recovery of silent tabs). `_async_stop` no longer awaits the sync `Browser.stop()`. Memory/all-stale restarts are offloaded to the recycle worker (no longer block the sweep loop). Adds regression tests in `tests_frame_router.py`, `tests_chrome_manager.py`, `tests_websocket.py`.
* Harden the TabManager lazy init race and fix the capped-429 contract: `_ensure_tab_manager()` now double-checks under a module-level `_tab_mgr_init_lock` so a racing first endpoint request and the first `_tab_manager_maintenance()` tick can no longer both build+`start()` a `ChromeManager` (the loser's shared-browser `user_data_dir` would linger forever in the orphan-sweeper registry); `ChromeManager.ensure_can_create_primary(url)` (new, mirrors `create_tab`'s non-shadow `len(_url_index) >= max_tabs` check) is now consulted synchronously by `FrameRouterService.ensure_and_fetch()` before spawning the background boot thread, so a capped manager surfaces the endpoint's 429 (`MaxTabsReachedError`) instead of a permanent 200 `"starting"` + a doomed boot thread per request — `_boot_tab`'s swallow-on-MaxTabs stays as a rarely-firing safety net; and `_tab_manager_maintenance()` now snapshots `_url_index` keys and `_tabs` values under `cm._lock` (RLock, never held across blocking/marshal calls) so the sweep thread can no longer hit `RuntimeError: dictionary changed size during iteration` while the loop thread / recycle worker mutate those dicts. Adds tests: cap-raises-without-boot vs free-slot-`"starting"` in `tests_frame_router_service.py`, a real-`FrameRouterService`-at-cap 429 for the flag-on endpoint in `tests_websocket.py`, and `ensure_can_create_primary` cap/shadow semantics in `tests_chrome_manager.py`.
* Wire `GET /websocket_messages` routing behind the `WS_TAB_MANAGER_ENABLED` flag: add a lazy `_tab_mgr` singleton + `_ensure_tab_manager()` (constructed on first request, never at import, so it never spawns Chrome at import time and the flag is testable via request-time env), route the endpoint to `FrameRouterService` when the flag is on (429 on `MaxTabsReachedError`) and keep the legacy `WebSocketListenerManager` path intact for rollback, and add `_tab_manager_maintenance()` (called each background sweep cycle) running the zombie watchdog (60s silence -> recycle), memory-triggered full restart (>1.2GB), max-lifetime recycle, and tab/loop gauges — all recycle work offloaded off the sweep loop.
* Reintroduce the bounded recycle executor behind a method seam (B1): `ChromeManager` gains a `ThreadPoolExecutor(max_workers=1, thread_name_prefix="tab_recycle")` created lazily in `__init__` (so tests never launch Chrome) and a `schedule_recycling(url, reason, func, *args)` public seam; `stop()` shuts it down with `wait=False, cancel_futures=True` (NOT wait=True — a worker blocked in `_call` on a shutting-down loop can stall up to LOOP_CALL_TIMEOUT/120s) and nulls it so `start()`/`schedule_recycling` can re-create it idempotently across `start()`/`stop()`/`restart_browser()` cycles; `_recover()` swaps the loop only and never touches the executor.
* Add a `FrameRouter.handoff` stale-handoff guard (B2): `ChromeManager.swap_primary` now returns the boolean CAS result (`True` if the url was re-pointed, `False` if a concurrent restart/recover already re-pointed it elsewhere). `handoff()` computes the CAS first under `url_lock`; on `False` it retires the shadow tab (never `old` — retiring it would drop the url's fresh primary) and aborts with `result="stale"` without re-draining, closing the recycle-mid-restart data-loss hole. `handoff` returns `"success"`/`"stale"`. Lock order `url_lock -> old.lock -> new.lock` and zero-drop dedup unchanged.
* Add `_recycle_tab` single-flight + per-URL cooldown (B3): acquires `cm._restart_lock` with `blocking=False` across the whole recycle (skips if a restart/recover is in progress), and uses a module-scoped `{url: datetime}` cooldown (armed before work, cleared only on success, 10-min back-off on failure) so a zombie + max-lifetime hit on the same tick cannot spawn a leaked running shadow. Keeps `warm_tab`/`handoff` + `WS_TAB_HANDOFF_TOTAL`/`WS_HANDOFF_DURATION` metrics, recording `result="stale"` when handoff aborts.
* Add endpoint-flag tests (flag off -> legacy, flag on -> TabManager stub routing, 429 on `MaxTabsReachedError`), `swap_primary` CAS-bool + executor lifecycle tests, the handoff stale-guard unit test, and `_recycle_tab` cooldown/single-flight tests.
* Add a thread-safe orphan-sweeper registration API in `flaresolverr_service.py`: `register_shared_browser_dir(path)` / `unregister_shared_browser_dir(path)` guard a module-level `_shared_browser_dirs` set behind a lock, and `_live_user_data_dirs()` now unions that set into its live-dir report so the sweeper never kills the shared single-Chrome user_data_dir. This is the proper replacement for the previous monkey-patch (which Task 4 removes from `_tab_manager_maintenance()`); `ChromeManager` already calls it defensively.
* Add `frame_router_service.py`: a thin `FrameRouterService` adapter exposing `ensure_and_fetch(url) -> {"status","messages":[...]}` that bridges `ChromeManager`+`FrameRouter` to the exact `/websocket_messages` response schema — the integration seam that makes the old and new managers interchangeable behind the `WS_TAB_MANAGER_ENABLED` flag. `status` maps to `starting`/`running`/`unhealthy`/`failed`; the first call for a URL kicks off background tab creation and reports `"starting"`, while the internal `cdp_ts` per-frame key (used only for cross-tab dedup) is stripped before messages are returned so they match `WebsocketMessage.__dict__` exactly (`{timestamp, type, url, payload}`). `MaxTabsReachedError` and generic create failures are swallowed with a WARNING/ERROR log and the per-URL pending set is cleared in a `finally`, so a failed boot never leaves the URL stuck in `"starting"`. Adds `tests_frame_router_service.py` covering the response schema, message key contract, `cdp_ts` stripping, and max-tabs warning + pending-clear behavior.
* Fix `FrameRouter.handoff` post-close re-drain: it previously called `ChromeManager.drain_tab(url)` AFTER `swap_primary` had re-pointed the url at the NEW tab, so it re-drained the new tab's buffer (a no-op clear/re-append that also double-counted `WS_MESSAGES_TOTAL`) while the OLD tab's trailing frames (captured between buffer-clear and async-close completion) were silently dropped when the old `TabState` retired. It now recovers the old tab's tail frames directly (copy+clear under `old_tab.lock`, still inside `url_lock`, then `new_tab.lock`), merging them into the new primary via the existing `_merge_dedup` so already-promoted frames are not re-leaked and no metric is double-counted, preserving the zero-drop and lock-order (`url_lock -> old.lock -> new.lock`) guarantees. Adds regression tests proving old-tail frames are recovered into `drain(url)` and that handoff no longer no-op-drains/clears the new primary.
* Add `frame_router.py`: a `FrameRouter` over the `ChromeManager` giving per-URL atomic drain, `tab_status` mapping to the `/websocket_messages` contract (`starting`/`running`/`unhealthy`/`failed`), and zero-drop `handoff` (shadow→primary promotion) that merges old+new tab buffers, deduping only genuine tab-overlap re-delivery via payload-structure keys + a shared-Chrome CDP timestamp window (`CDP_OVERLAP_WINDOW_S=2.0`). Never collapses distinct messages: keyless frames and frames without CDP timestamps are always kept (zero-drop). Adds `tests_frame_router.py` covering drain/clear, overlap + disjoint handoff, mevx jsonrpc-id / flash-snapshot dedup, far-in-time duplicate preservation, and the `tab_status` contract mapping.
* Fix legacy `WS_*` metric accounting in `ChromeManager` so existing dashboards stay accurate under the single-shared-Chrome path: gauges are now derived from the registry (never blind inc/dec) — `retire_tab_id` keeps `WS_LISTENER_ACTIVE=1` (and the global count unchanged) while a url still has a live primary after `swap_primary` handoff, drops it only when the last tab for the url is retired; `_recover()`, `stop()` and `restart_browser()` recompute via the new `_recompute_ws_gauges()`/`_sync_url_metrics()` helpers so restarts can no longer inflate `WS_LISTENERS_ACTIVE`. `WS_LISTENER_UPTIME` is re-marked only when a url BECOMES a live primary (first create or post-recover recreate), not by shadow/warming creates, and stays continuous across handoffs. `start()` now sets `_running` before launching, winds the loop thread down and re-raises if Chrome launch fails (was: unrecoverable spinning daemon loop). Removed the never-used `_recycle_executor`. Adds `tests_chrome_manager.py` cases covering handoff-retire gauge/uptime continuity and stop+recreate gauge accuracy.
* Add `chrome_manager.py`: a single shared nodriver Chrome with multi-tab CDP websocket capture, gated behind the new `WS_TAB_MANAGER_ENABLED` config getter (`get_config_ws_tab_manager_enabled()`). `ChromeManager` runs nodriver on a dedicated asyncio loop in a daemon thread, marshalling calls via `asyncio.run_coroutine_threadsafe`, with a loop-crash watchdog (`_recover()`), a `max_tabs` cap (one shadow per primary for zero-drop handoff), `create/warm/retire/drain/swap` tab APIs, a 1MB frame-size guard, `--site-per-process` renderer isolation, and `WS_CHROME_SANDBOX` (default enabled) sandbox control. Registers/unregisters its shared browser `user_data_dir` with the orphan sweeper (defensively guarded against the not-yet-present API). Adds tab/lifecycle metrics (`WS_TABS_ACTIVE`, `WS_TABS_RUNNING`, `WS_TAB_HANDOFF_TOTAL`, `WS_TAB_RESTART_TOTAL`, `WS_TAB_AGE`, `WS_FRAME_BUFFER_UTILIZATION`, `WS_HANDOFF_DURATION`, `WS_LOOP_THREAD_ALIVE`) — the pre-existing legacy WS gauges are unchanged. New `tests_chrome_manager.py` logic test suite (stubbed nodriver).
* Final-review fix wave: profile-dir sweep safety escalation — `sweep_stale_profile_dirs` now caps deletions at `MAX_SWEEPS_PER_CYCLE = 200` per invocation (a 2,200-dir backlog drains over ~12 min instead of stalling the shared background thread in one burst) and the wire-in logs `profile dir sweep removed %d stale dirs` at INFO when anything was removed; `max_age_seconds=0` is now honored explicitly (previously `or`-falsified into the config default).
* Final-review fix wave: idempotent-quit retry semantics preserved — `_quitted = True` moved from the top to the very end of `Chrome.quit()`, so a quit that raises mid-way can be retried instead of every subsequent call silently no-oping (early-return guard unchanged; no recursion hazard since quit never calls itself).
* Final-review fix wave: UC Options-reuse RuntimeError guard preserved after collection — `quit()` now severs the back-reference with `delattr(self.options, '_session')` instead of `= None`, keeping the `hasattr(options, "_session")` class-level default intact so reusing a collected driver's options still raises as upstream intends.
* Final-review fix wave: `background_tasks_thread` initializes `live = set()` before the orphan-kill try block so a failed `_live_user_data_dirs()` can no longer leave `live` unbound/stale for the subsequent profile sweep; watchdog test now asserts the sweep receives exactly the live dir set.
* New gauge `flaresolverr_unquit_chrome_drivers` set by `update_lifecycle_gauges()` to `len(uc.LIVE_CHROMES)` — tracks Chrome instances that never reached `quit()` (force-killed at exit); should stay flat near the listener count, monotonic growth = driver-retention regression.
* Add optional periodic gc escape hatch: `ENABLE_PERIODIC_GC` env knob (default `false`, getter `get_config_enable_periodic_gc()`) makes the background sweep thread run `gc.collect()` each cycle when enabled.
* Tests: TestLifecycleGauges gains unquit-driver gauge test (weakref-able plain-class fake added to LIVE_CHROMES); TestDriverTimeoutHardening gains `get_config_enable_periodic_gc` default/override test.
* Add `sweep_stale_profile_dirs()`: removes `flaresolverr_*` chromium profile dirs under the tempdir that are non-live (not owned by any active session/listener), have no running chrome process behind them, and are older than `PROFILE_DIR_MAX_AGE_S` (default 600s). `kill_orphaned_chrome()` only reaches dirs whose browser is still running, so dirs of already-dead drivers accumulated indefinitely (2,200+ observed in production); the sweeper runs best-effort per-entry in the existing background sweep loop right after the orphan-chrome kill block, reusing its live-dir set.
* Sever remaining post-quit reference cycles in `Chrome.quit()` so quitted drivers are reclaimed by refcount alone (completing the Task-1 unpin): after the reactor stop/join block, `self.reactor.handlers.clear()` + `self.reactor = None` breaks Chrome↔Reactor, and `self.options._session = None` breaks the Options back-reference. Safe: `add/remove_cdp_listener` short-circuit on `reactor is not None`, and listener CDP removal always runs before `safe_quit`. Tests: TestChromeRetention gains source-check plus behavioral test (SimpleNamespace fakes, `os.kill` patched).
* Fix Chrome-object retention leak: replace `finalize(self, self._ensure_close, self)` in `Chrome.__init__` (whose `weakref.finalize._registry` args tuple held `self` STRONGLY, pinning every driver for process lifetime) with a module-level `LIVE_CHROMES: weakref.WeakSet` registered as the last statement of `__init__` plus an `atexit` killer `_kill_unquit_chromes()` that force-kills un-quit chromedrivers at interpreter exit. `quit()` now discards itself from the set (best-effort) and gains idempotency via a `_quitted` sentinel (also neutralizes the post-fix `__del__`-driven second quit whose stale-pid `os.kill(browser_pid)` could hit a recycled pid); browser_pid is nulled at end of quit.
* Fix safe_quit escalation-vs-cancel race: if `driver.quit()` is still alive past grace*2, log a WARNING ("escalation performed/armed") and leave the escalation Timer uncancelled — cancelling unconditionally raced the Timer's own (grace*2) deadline, so a wedged chromedriver could silently leak. Exception propagation semantics unchanged.
* `cleanup_stale_sessions` now isolates per-session destroy failures with try/except + warning so one poisoned session cannot abort the remaining sweep.
* `_escalate_kill` logs a WARNING at entry (`safe_quit escalation: killing chromedriver pid=... browser_pid=... profile=...`).
* Fix malformed reactor logging calls: `logging.debug("event dispatch error:", e)` / `"exception ignored :"` → proper lazy-formatted `logger.debug("...: %s", e)`.
* New label-free gauge `flaresolverr_process_rss_bytes` set by `update_lifecycle_gauges()` from `/proc/self/statm` field 2 × page size (best-effort, 0 on non-Linux).
* Tests: TestLifecycleGauges covers all four lifecycle gauges plus the RSS series (dummy ThreadPoolExecutor-named thread, live asyncio loop held across the call, GC_CHROME_DRIVERS series presence); flaky pre-call thread-count assertion removed.
* Deterministic reactor lifecycle + observability gauges: `Reactor.run()` tears down asyncgens/default-executor and closes the event loop in a `finally` block on the owner thread; `Chrome.quit()` sets the reactor stop-event and joins it (bounded 2s) before killing chromedriver/service; CDP handler dispatch is synchronous with per-handler error isolation so one handler exception no longer aborts the rest of the poll batch. Adds four label-free lifecycle gauges (`flaresolverr_process_threads_active`, `flaresolverr_thread_pool_workers`, `flaresolverr_gc_event_loops`, `flaresolverr_gc_chrome_drivers`) sampled by the background sweep loop.
* Add `safe_quit()` / `_escalate_kill()` in utils: driver teardown that cannot hang forever. The chromedriver shutdown-command runs on a daemon thread abandoned after `SHUTDOWN_GRACE` (default 10s), `quit()` itself is bounded at grace*2 with its exception re-raised if it completes, and an escalation timer force-kills chromedriver/browser pids (TERM→KILL) and sweeps the profile dir if quit hasn't returned in time. Fixes wedged `destroy()` calls leaking whole Chrome process trees when UC's un-timeouted `/shutdown` urlopen hangs. Switched hot teardown call sites (sessions destroy, _resolve_challenge finally, get_user_agent finally); startup launch-test path keeps plain `quit()`.
* Bound selenium blocking at driver creation: every browser launched via `get_webdriver()` now gets a bounded HTTP command timeout (`DRIVER_COMMAND_TIMEOUT`, default 120s) on the urllib3 client-config (previously `None` in pinned selenium 4.39.0, so a hung chromedriver froze a thread forever), plus self-bounding page loads (`PAGE_LOAD_TIMEOUT`, default 75s < 90s listener-create bound) and script timeouts via new `_harden_driver_timeouts()`. Adds config getters `get_config_driver_command_timeout()`, `get_config_page_load_timeout()`, `get_config_shutdown_grace()` (default 10s, consumed by upcoming safe_quit work).
* Add zero-drop listener recycling: listeners are proactively recycled after `WS_LISTENER_MAX_LIFETIME_MINUTES` (default 180 min) via spawn-first handover — a replacement browser is launched in parallel, ownership of the URL slot swaps atomically on success, and buffers are merged so no captured frames are dropped. Single-flight mutex prevents concurrent recycles of the same listener.
* Tune Chromium flags for long-lived headless listeners and preserve frame buffers across reconnects; ownership-safe index pops prevent recycle threads from racing the manager.
* Derive listener gauges (`WS_LISTENERS_ACTIVE`, `WS_LISTENERS_RUNNING`, `WS_LISTENERS_STATUS`) from primary (indexed) listeners only so recycle shadows are invisible to dashboards; per-URL metrics now iterate `_url_index` directly.
* Anchor `WS_LISTENER_UPTIME` at `service_started_at` (falling back to `created_at`) so uptime survives recycles.
* Fix FIFO ordering of frames re-drained during the final pre-retire merge in `_recycle_listener` (append instead of prepend).
* Add `target_url` field to `Session` and `SessionsStorage.create()` so websocket listener sessions carry their target URL.
* Fall back to the listener's `target_url` when CDP websocket events lack a URL, eliminating metrics recorded with an empty `url=""` label.
* Add six `flaresolverr_ws_*` Prometheus metrics for WebSocket-listener stability (active listeners, per-status count, lifecycle events, reconnect outcomes, frames captured, session duration) and wire them into WebSocketListenerManager lifecycle.
* Give every browser an explicit `--user-data-dir` (`flaresolverr_` prefix) and reap the partially-spawned Chromium via `pkill` + `rmtree` when `uc.Chrome` fails to launch, fixing the launch-failure memory leak. Add `kill_orphaned_chrome()` watchdog helper to reap stale orphaned profiles.

## v3.5.0 (2026/05/26)
* Add formatting to log file
* Resolve turnstile captcha. Thanks @denis-svg
* Bump dependencies. Thanks @flower
* Fix tar.gz having wrong uid/gid. Thanks @NikoCat233
* Revert base image & Python

## v3.4.6 (2025/11/29)
* Add disable image, css, fonts option with CDP. Thanks @Ananto30

## v3.4.5 (2025/11/11)
* Revert to Python v3.13

## v3.4.4 (2025/11/04)
* Bump dependencies, Chromium, and some other general fixes. Thanks @flowerey

## v3.4.3 (2025/10/28)
* Update proxy extension

## v3.4.2 (2025/10/09)
* Bump dependencies & CI actions. Thanks @flowerey
* Add optional wait time after resolving the challenge before returning. Thanks @kennedyoliveira
* Add proxy ENVs. Thanks @Robokishan
* Handle empty string and keys without value in postData. Thanks @eZ4RK0
* Add quote protection for password containing it. Thanks @warrenberberd
* Add returnScreenshot parameter to screenshot the final web page. Thanks @estebanthi
* Add log file support. Thanks @acg5159

## v3.4.1 (2025/09/15)
* Fix regex pattern syntax in utils.py
* Change access denied title check to use startswith

## v3.4.0 (2025/08/25)
* Modernize and upgrade application. Thanks @TheCrazyLex
* Remove disable software rasterizer option for ARM builds. Thanks @smrodman83

## v3.3.25 (2025/06/14)
* Remove `use-gl` argument. Thanks @qwerty12
* u_c: remove apparent c&p typo. Thanks @ok3721
* Bump requirements

## v3.3.24 (2025/06/04)
* Remove hidden character

## v3.3.23 (2025/06/04)
* Update base image to bookworm. Thanks @rwjack

## v3.3.22 (2025/06/03)
* Disable search engine choice screen
* Fix headless=false stalling. Thanks @MAKMED1337
* Change from click to keys. Thanks @sh4dowb
* Don't open devtools
* Bump Chromium to v137 for build
* Bump requirements

## v3.3.21 (2024/06/26)
* Add challenge selector to catch reloading page on non-English systems
* Escape values for generated form used in request.post. Thanks @mynameisbogdan

## v3.3.20 (2024/06/21)
* maxTimeout should always be int
* Check not running in Docker before logging version_main error
* Update Cloudflare challenge and checkbox selectors. Thanks @tenettow & @21hsmw

## v3.3.19 (2024/05/23)
* Fix occasional headless issue on Linux when set to "false". Thanks @21hsmw

## v3.3.18 (2024/05/20)

* Fix LANG ENV for Linux
* Fix Chrome v124+ not closing on Windows. Thanks @RileyXX

## v3.3.17 (2024/04/09)

* Fix file descriptor leak in service on quit(). Thanks @zkulis

## v3.3.16 (2024/02/28)

* Fix of the subprocess.STARTUPINFO() call. Thanks @ceconelo
* Add FreeBSD support. Thanks @Asthowen
* Use headless configuration properly. Thanks @hashworks

## v3.3.15 (2024/02/20)

* Fix looping challenges

## v3.3.14-hotfix2 (2024/02/17)

* Hotfix 2 - bad Chromium build, instances failed to terminate

## v3.3.14-hotfix (2024/02/17)

* Hotfix for Linux build - some Chrome files no longer exist

## v3.3.14 (2024/02/17)

* Update Chrome downloads. Thanks @opemvbs

## v3.3.13 (2024/01/07)

* Fix too many open files error

## v3.3.12 (2023/12/15)

* Fix looping challenges and invalid cookies

## v3.3.11 (2023/12/11)

* Update UC 3.5.4 & Selenium 4.15.2. Thanks @txtsd

## v3.3.10 (2023/11/14)

* Add LANG ENV - resolves issues with YGGtorrent

## v3.3.9 (2023/11/13)

* Fix for Docker build, capture TypeError

## v3.3.8 (2023/11/13)

* Fix headless=true for Chrome 117+. Thanks @NabiKAZ
* Support running Chrome 119 from source. Thanks @koleg and @Chris7X
* Fix "OSError: [WinError 6] The handle is invalid" on exit. Thanks @enesgorkemgenc

## v3.3.7 (2023/11/05)

* Bump to rebuild. Thanks @JoachimDorchies

## v3.3.6 (2023/09/15)

* Update checkbox selector, again

## v3.3.5 (2023/09/13)

* Change checkbox selector, support languages other than English

## v3.3.4 (2023/09/02)

* Update checkbox selector

## v3.3.3 (2023/08/31)

* Update undetected_chromedriver to v3.5.3

## v3.3.2 (2023/08/03)

* Fix URL domain in Prometheus exporter

## v3.3.1 (2023/08/03)

* Fix for Cloudflare verify checkbox
* Fix HEADLESS=false in Windows binary
* Fix Prometheus exporter for management and health endpoints
* Remove misleading stack trace when the verify checkbox is not found
* Revert "Update base Docker image to Debian Bookworm" #849
* Revert "Install Chromium 115 from Debian testing" #849

## v3.3.0 (2023/08/02)

* Fix for new Cloudflare detection. Thanks @cedric-bour for #845
* Add support for proxy authentication username/password. Thanks @jacobprice808	for #807
* Implement Prometheus metrics
* Fix Chromium Driver for Chrome / Chromium version > 114
* Use Chromium 115 in binary packages (Windows and Linux)
* Install Chromium 115 from Debian testing (Docker)
* Update base Docker image to Debian Bookworm
* Update Selenium 4.11.2
* Update pyinstaller 5.13.0
* Add more traces in build_package.py

## v3.2.2 (2023/07/16)

* Workaround for updated 'verify you are human' check

## v3.2.1 (2023/06/10)

* Kill dead Chrome processes in Windows
* Fix Chrome GL erros in ASUSTOR NAS

## v3.2.0 (2023/05/23)

* Support "proxy" param in requests and sessions
* Support "cookies" param in requests
* Fix Chromium exec permissions in Linux package
* Update Python dependencies

## v3.1.2 (2023/04/02)

* Fix headless mode in macOS
* Remove redundant artifact from Windows binary package
* Bump Selenium dependency

## v3.1.1 (2023/03/25)

* Distribute binary executables in compressed package
* Add icon for binary executable
* Include information about supported architectures in the readme
* Check Python version on start

## v3.1.0 (2023/03/20)

* Build binaries for Linux x64 and Windows x64
* Sessions with auto-creation on fetch request and TTL
* Fix error trace: Crash Reports/pending No such file or directory
* Fix Waitress server error with asyncore_use_poll=true
* Attempt to fix Docker ARM32 build
* Print platform information on start up
* Add Fairlane challenge selector
* Update DDOS-GUARD title
* Update dependencies

## v3.0.4 (2023/03/07)

* Click on the Cloudflare's 'Verify you are human' button if necessary

## v3.0.3 (2023/03/06)

* Update undetected_chromedriver version to 3.4.6

## v3.0.2 (2023/01/08)

* Detect Cloudflare blocked access
* Check Chrome / Chromium web browser is installed correctly

## v3.0.1 (2023/01/06)

* Kill Chromium processes properly to avoid defunct/zombie processes
* Update undetected-chromedriver
* Disable Zygote sandbox in Chromium browser
* Add more selectors to detect blocked access
* Include procps (ps), curl and vim packages in the Docker image

## v3.0.0 (2023/01/04)

* This is the first release of FlareSolverr v3. There are some breaking changes
* Docker images for linux/386, linux/amd64, linux/arm/v7 and linux/arm64/v8
* Replaced Firefox with Chrome
* Replaced NodeJS / Typescript with Python
* Replaced Puppeter with Selenium
* No binaries for Linux / Windows. You have to use the Docker image or install from Source code
* No proxy support
* No session support

## v2.2.10 (2022/10/22)

* Detect DDoS-Guard through title content

## v2.2.9 (2022/09/25)

* Detect Cloudflare Access Denied
* Commit the complete changelog

## v2.2.8 (2022/09/17)

* Remove 30 s delay and clean legacy code

## v2.2.7 (2022/09/12)

* Temporary fix: add 30s delay
* Update README.md

## v2.2.6 (2022/07/31)

* Fix Cloudflare detection in POST requests

## v2.2.5 (2022/07/30)

* Update GitHub actions to build executables with NodeJs 16
* Update Cloudflare selectors and add HTML samples
* Install Firefox 94 instead of the latest Nightly
* Update dependencies
* Upgrade Puppeteer (#396)

## v2.2.4 (2022/04/17)

* Detect DDoS-Guard challenge

## v2.2.3 (2022/04/16)

* Fix 2000 ms navigation timeout
* Update README.md (libseccomp2 package in Debian)
* Update README.md (clarify proxy parameter) (#307)
* Update NPM dependencies
* Disable Cloudflare ban detection

## v2.2.2 (2022/03/19)

* Fix ban detection. Resolves #330 (#336)

## v2.2.1 (2022/02/06)

* Fix max timeout error in some pages
* Avoid crashing in NodeJS 17 due to Unhandled promise rejection
* Improve proxy validation and debug traces
* Remove @types/puppeteer dependency

## v2.2.0 (2022/01/31)

* Increase default BROWSER_TIMEOUT=40000 (40 seconds)
* Fix Puppeter deprecation warnings
* Update base Docker image Alpine 3.15 / NodeJS 16
* Build precompiled binaries with NodeJS 16
* Update Puppeter and other dependencies
* Add support for Custom CloudFlare challenge
* Add support for DDoS-GUARD challenge

## v2.1.0 (2021/12/12)

* Add aarch64 to user agents to be replaced (#248)
* Fix SOCKSv4 and SOCKSv5 proxy. resolves #214 #220
* Remove redundant JSON key (postData) (#242)
* Make test URL configurable with TEST_URL env var. resolves #240
* Bypass new Cloudflare protection
* Update donation links

## v2.0.2 (2021/10/31)

* Fix SOCKS5 proxy. Resolves #214
* Replace Firefox ERS with a newer version
* Catch startup exceptions and give some advices
* Add env var BROWSER_TIMEOUT for slow systems
* Fix NPM warning in Docker images

## v2.0.1 (2021/10/24)

* Check user home dir before testing web browser installation

## v2.0.0 (2021/10/20)

FlareSolverr 2.0.0 is out with some important changes:

* It is capable of solving the automatic challenges of Cloudflare. CAPTCHAs (hCaptcha) cannot be resolved and the old solvers have been removed.
* The Chrome browser has been replaced by Firefox. This has caused some functionality to be removed. Parameters: `userAgent`, `headers`, `rawHtml` and `downloadare` no longer available.
* Included `proxy` support without user/password credentials. If you are writing your own integration with FlareSolverr, make sure your client uses the same User-Agent header and Proxy that FlareSolverr uses. Those values together with the Cookie are checked and detected by Cloudflare.
* FlareSolverr has been rewritten from scratch. From now on it should be easier to maintain and test.
* If you are using Jackett make sure you have version v0.18.1041 or higher. FlareSolverSharp v2.0.0 is out too.

Complete changelog:

* Bump version 2.0.0
* Set puppeteer timeout half of maxTimeout param. Resolves #180
* Add test for blocked IP
* Avoid reloading the page in case of error
* Improve Cloudflare detection
* Fix version
* Fix browser preferences and proxy
* Fix request.post method and clean error traces
* Use Firefox ESR for Docker images
* Improve Firefox start time and code clean up
* Improve bad request management and tests
* Build native packages with Firefox
* Update readme
* Improve Docker image and clean TODOs
* Add proxy support
* Implement request.post method for Firefox
* Code clean up, remove returnRawHtml, download, headers params
* Remove outdated chaptcha solvers
* Refactor the app to use Express server and Jest for tests
* Fix Cloudflare resolver for Linux ARM builds
* Fix Cloudflare resolver
* Replace Chrome web browser with Firefox
* Remove userAgent parameter since any modification is detected by CF
* Update dependencies
* Remove Puppeter steath plugin

## v1.2.9 (2021/08/01)

* Improve "Execution context was destroyed" error handling
* Implement returnRawHtml parameter. resolves #172 resolves #165
* Capture Docker stop signal. resolves #158
* Reduce Docker image size 20 MB
* Fix page reload after challenge is solved. resolves #162 resolves #143
* Avoid loading images/css/fonts to speed up page load
* Improve Cloudflare IP ban detection
* Fix vulnerabilities

## v1.2.8 (2021/06/01)

* Improve old JS challenge waiting. Resolves #129

## v1.2.7 (2021/06/01)

* Improvements in Cloudflare redirect detection. Resolves #140
* Fix installation instructions

## v1.2.6 (2021/05/30)

* Handle new Cloudflare challenge. Resolves #135 Resolves #134
* Provide reference Systemd unit file. Resolves #72
* Fix EACCES: permission denied, open '/tmp/flaresolverr.txt'. Resolves #120
* Configure timezone with TZ env var. Resolves #109
* Return the redirected URL in the response (#126)
* Show an error in hcaptcha-solver. Resolves #132
* Regenerate package-lock.json lockfileVersion 2
* Update issue template. Resolves #130
* Bump ws from 7.4.1 to 7.4.6 (#137)
* Bump hosted-git-info from 2.8.8 to 2.8.9 (#124)
* Bump lodash from 4.17.20 to 4.17.21 (#125)

## v1.2.5 (2021/04/05)

* Fix memory regression, close test browser
* Fix release-docker GitHub action

## v1.2.4 (2021/04/04)

* Include license in release zips. resolves #75
* Validate Chrome is working at startup
* Speedup Docker image build
* Add health check endpoint
* Update issue template
* Minor improvements in debug traces
* Validate environment variables at startup. resolves #101
* Add FlareSolverr logo. resolves #23

## v1.2.3 (2021/01/10)

* CI/CD: Generate release changelog from commits. resolves #34
* Update README.md
* Add donation links
* Simplify docker-compose.yml
* Allow to configure "none" captcha resolver
* Override docker-compose.yml variables via .env resolves #64 (#66)

## v1.2.2 (2021/01/09)

* Add documentation for precompiled binaries installation
* Add instructions to set environment variables in Windows
* Build Windows and Linux binaries. resolves #18
* Add release badge in the readme
* CI/CD: Generate release changelog from commits. resolves #34
* Add a notice about captcha solvers
* Add Chrome flag --disable-dev-shm-usage to fix crashes. resolves #45
* Fix Docker CLI documentation
* Add traces with captcha solver service. resolves #39
* Improve logic to detect Cloudflare captcha. resolves #48
* Move Cloudflare provider logic to his own class
* Simplify and document the "return only cookies" parameter
* Show message when debug log is enabled
* Update readme to add more clarifications. resolves #53 (#60)
* issue_template: typo fix (#52)

## v1.2.1 (2020/12/20)

* Change version to match release tag / 1.2.0 => v1.2.0
* CI/CD Publish release in GitHub repository. resolves #34
* Add welcome message in / endpoint
* Rewrite request timeout handling (maxTimeout) resolves #42
* Add http status for better logging
* Return an error when no selectors are found, #25
* Add issue template, fix #32
* Moving log.html right after loading the page and add one on reload, fix #30
* Update User-Agent to match chromium version, ref: #15 (#28)
* Update install from source code documentation
* Update readme to add Docker instructions (#20)
* Clean up readme (#19)
* Add docker-compose
* Change default log level to info

## v1.2.0 (2020/12/20)

* Fix User-Agent detected by CouldFlare (Docker ARM) resolves #15
* Include exception message in error response
* CI/CD: Rename GitHub Action build => publish
* Bump version
* Fix TypeScript compilation and bump minor version
* CI/CD: Bump minor version
* CI/CD: Configure GitHub Actions
* CI/CD: Configure GitHub Actions
* CI/CD: Bump minor version
* CI/CD: Configure Build GitHub Action
* CI/CD: Configure AutoTag GitHub Action (#14)
* CI/CD: Build the Docker images with GitHub Actions (#13)
* Update dependencies
* Backport changes from Cloudproxy (#11)
