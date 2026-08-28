# FlareSolverr-websocket/src/frame_router_service.py
import logging
import threading
import time
from chrome_manager import MaxTabsReachedError

logger = logging.getLogger(__name__)


def _web_socket_message_dict(timestamp, type, url, payload):
    """Mirrors WebsocketMessage.__dict__ exactly (flaresolverr_service.py)."""
    return {
        "timestamp": timestamp,
        "type": type,
        "url": url,
        "payload": payload,
    }


class FrameRouterService:
    """Adapters ChromeManager+FrameRouter to the /websocket_messages contract."""

    def __init__(self, chrome_manager, frame_router):
        self.mgr = chrome_manager
        self.router = frame_router
        self._pending: set = set()          # urls currently being (re)created
        self._lock = threading.Lock()

    def ensure_and_fetch(self, url: str):
        tab = self.mgr.get_tab(url)
        if tab is None or getattr(tab, "status", "") == "crashed":
            if tab is not None and getattr(tab, "status", "") == "crashed":
                try:
                    self.mgr.retire_tab(url)
                except Exception:
                    pass
            self.mgr.ensure_can_create_primary(url)
            with self._lock:
                if url not in self._pending:
                    self._pending.add(url)
                    threading.Thread(target=self._boot_tab, args=(url,),
                                     daemon=True).start()
            return {"status": "starting", "messages": []}

        status = self.router.tab_status(url)
        frames = self.router.drain(url)
        # Strip the INTERNAL cdp_ts field so the returned messages match the
        # existing contract exactly: {timestamp, type, url, payload}.
        messages = [
            _web_socket_message_dict(f.get("timestamp"), f.get("type"),
                                     f.get("url"), f.get("payload"))
            for f in frames
        ]
        return {"status": status, "messages": messages}

    def _boot_tab(self, url: str):
        try:
            # Single-flight against restart/recover (quorum B): creating a tab
            # while restart_browser()/_recover() is re-registering primaries
            # races the registry. Non-blocking acquire — if a restart/recover is
            # in progress we skip this URL (the fresh primary is re-created by the
            # restart/recover; the client's next poll re-boots if still absent).
            # Guarded with getattr so lightweight test doubles that lack the lock
            # still pass through.
            lock = getattr(self.mgr, "_restart_lock", None)
            if lock is not None and not lock.acquire(blocking=False):
                logger.info(f"FrameRouterService: boot for {url} deferred (restart/recover in progress)")
                return
            try:
                self.mgr.create_tab(url)
            finally:
                if lock is not None:
                    lock.release()
        except MaxTabsReachedError:
            logger.warning(f"FrameRouterService: max tabs reached for {url}")
        except Exception as e:
            logger.error(f"FrameRouterService: tab boot failed for {url}: {e}")
        finally:
            with self._lock:
                self._pending.discard(url)