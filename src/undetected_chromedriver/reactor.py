#!/usr/bin/env python3
# this module is part of undetected_chromedriver

import asyncio
import json
import logging
import threading


logger = logging.getLogger(__name__)


class Reactor(threading.Thread):
    def __init__(self, driver: "Chrome"):
        super().__init__()

        self.driver = driver
        self.loop = asyncio.new_event_loop()

        self.lock = threading.Lock()
        self.event = threading.Event()
        self.daemon = True
        self.handlers = {}

    def add_event_handler(self, method_name, callback: callable):
        """

        Parameters
        ----------
        event_name: str
            example "Network.responseReceived"

        callback: callable
            callable which accepts 1 parameter: the message object dictionary

        Returns
        -------

        """
        with self.lock:
            self.handlers[method_name.lower()] = callback

    @property
    def running(self):
        return not self.event.is_set()

    def run(self):
        try:
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.listen())
        except Exception as e:
            logger.warning("Reactor.run() => %s", e)
        finally:
            # Deterministic teardown: this thread owns the loop; never close
            # cross-thread from quit(). bpo-41699 hazard class avoided.
            for coro_name in ("shutdown_asyncgens", "shutdown_default_executor"):
                try:
                    self.loop.run_until_complete(getattr(self.loop, coro_name)())
                except Exception:
                    logger.debug("loop %s failed", coro_name, exc_info=True)
            try:
                self.loop.close()
            except Exception:
                pass

    async def _wait_service_started(self):
        while True:
            with self.lock:
                if (
                    getattr(self.driver, "service", None)
                    and getattr(self.driver.service, "process", None)
                    and self.driver.service.process.poll()
                ):
                    await asyncio.sleep(self.driver._delay or 0.25)
                else:
                    break

    async def listen(self):
        while self.running:
            await self._wait_service_started()
            await asyncio.sleep(1)

            try:
                with self.lock:
                    log_entries = self.driver.get_log("performance")

                for entry in log_entries:
                    try:
                        obj_serialized: str = entry.get("message")
                        obj = json.loads(obj_serialized)
                        message = obj.get("message")
                        method = message.get("method")

                        cb = self.handlers.get("*") or self.handlers.get(method.lower())
                        if cb is not None:
                            # Synchronous call: dispatch was already fully serial
                            # (each await completed before the next entry), so the
                            # executor added only a stranded-thread hazard. A
                            # handler error must not abort the remaining batch.
                            try:
                                cb(message)
                            except Exception as e:
                                logger.debug("cdp handler error (%s): %s", method, e)
                    except Exception as e:
                        logger.debug("event dispatch error: %s", e)

            except Exception as e:
                if "invalid session id" in str(e):
                    pass
                else:
                    logger.debug("exception ignored: %s", e)
