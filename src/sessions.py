from collections import deque
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Tuple
from uuid import uuid1

from selenium.webdriver.chrome.webdriver import WebDriver

import utils

SESSION_INACTIVITY_TIMEOUT = timedelta(minutes=30)
SESSION_MAX_LIFETIME = timedelta(minutes=30)


@dataclass
class Session:
    session_id: str
    driver: WebDriver
    created_at: datetime
    last_request_timestamp: datetime = field(default_factory=datetime.now)
    websocket_messages: deque = field(default_factory=lambda: deque(maxlen=utils.get_config_websocket_max_messages()))
    last_known_url: str = ""
    url_cache_time: float = 0
    target_url: str = ""

    def lifetime(self) -> timedelta:
        return datetime.now() - self.created_at


class SessionsStorage:
    """SessionsStorage creates, stores and process all the sessions"""

    def __init__(self):
        self.sessions = {}

    def create(self, session_id: Optional[str] = None, proxy: Optional[dict] = None,
               force_new: Optional[bool] = False, target_url: str = "") -> Tuple[Session, bool]:
        """create creates new instance of WebDriver if necessary,
        assign defined (or newly generated) session_id to the instance
        and returns the session object. If a new session has been created
        second argument is set to True.

        Note: The function is idempotent, so in case if session_id
        already exists in the storage a new instance of WebDriver won't be created
        and existing session will be returned. Second argument defines if 
        new session has been created (True) or an existing one was used (False).
        """
        session_id = session_id or str(uuid1())

        if force_new:
            self.destroy(session_id)

        if self.exists(session_id):
            return self.sessions[session_id], False

        driver = utils.get_webdriver(proxy)
        created_at = datetime.now()
        session = Session(session_id, driver, created_at, target_url=target_url)

        self.sessions[session_id] = session

        return session, True

    def exists(self, session_id: str) -> bool:
        return session_id in self.sessions

    def destroy(self, session_id: str) -> bool:
        """destroy closes the driver instance and removes session from the storage.
        The function is noop if session_id doesn't exist.
        The function returns True if session was found and destroyed,
        and False if session_id wasn't found.
        """
        if not self.exists(session_id):
            return False

        session = self.sessions.pop(session_id)
        if utils.PLATFORM_VERSION == "nt":
            session.driver.close()
        utils.safe_quit(session.driver)
        return True

    def get(self, session_id: str, ttl: Optional[timedelta] = None) -> Tuple[Session, bool]:
        session, fresh = self.create(session_id)

        if ttl is not None and not fresh and session.lifetime() > ttl:
            logging.debug(f'session\'s lifetime has expired, so the session is recreated (session_id={session_id})')
            session, fresh = self.create(session_id, force_new=True)
        
        session.last_request_timestamp = datetime.now()

        return session, fresh

    def session_ids(self) -> list[str]:
        return list(self.sessions.keys())

    def cleanup_stale_sessions(self):
        logging.debug("Cleaning up stale sessions...")
        session_ids_to_destroy = []
        now = datetime.now()
        for session_id, session in self.sessions.items():
            if session_id.startswith("ws_listener_"):
                continue
            if (now - session.last_request_timestamp) > SESSION_INACTIVITY_TIMEOUT:
                logging.info(
                    f"Session {session_id} inactive for too long. Last activity: {session.last_request_timestamp}. Destroying."
                )
                session_ids_to_destroy.append(session_id)
            elif session.lifetime() > SESSION_MAX_LIFETIME:
                logging.info(
                    f"Session {session_id} exceeded maximum lifetime. Created at: {session.created_at}. Destroying."
                )
                session_ids_to_destroy.append(session_id)
        
        for session_id in session_ids_to_destroy:
            try:
                self.destroy(session_id)
            except Exception as e:
                logging.warning(
                    f"Failed to destroy stale session {session_id}: {e}")
        logging.debug(f"Cleaned up {len(session_ids_to_destroy)} stale sessions.")
