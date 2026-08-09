import unittest
from collections import deque
from types import SimpleNamespace
from unittest.mock import patch

from webtest import TestApp

import flaresolverr
import flaresolverr_service


class TestWebsocketCapture(unittest.TestCase):
    def setUp(self):
        self.app = TestApp(flaresolverr.app)
        self.original_messages = flaresolverr.WEBSOCKET_MESSAGES
        flaresolverr.WEBSOCKET_MESSAGES = deque(maxlen=100)

    def tearDown(self):
        flaresolverr.WEBSOCKET_MESSAGES = self.original_messages

    def test_global_endpoint_returns_and_clears_messages(self):
        flaresolverr.WEBSOCKET_MESSAGES.append(
            flaresolverr_service.WebsocketMessage(1.0, "received", "wss://example.test", "payload")
        )

        response = self.app.get('/websocket_messages')
        self.assertEqual(response.json[0]["type"], "received")
        self.assertEqual(self.app.get('/websocket_messages').json, [])

    def test_session_endpoint_returns_and_clears_session_queue(self):
        session = SimpleNamespace(websocket_messages=deque([
            flaresolverr_service.WebsocketMessage(2.0, "sent", "wss://example.test", "out")
        ]))
        with patch.object(flaresolverr_service.SESSIONS_STORAGE, "get", return_value=(session, False)):
            response = self.app.get('/websocket_messages?session=session-1')

        self.assertEqual(response.json[0]["payload"], "out")
        self.assertEqual(len(session.websocket_messages), 0)

    def test_capture_extracts_frame_type_url_and_payload(self):
        session = SimpleNamespace(
            websocket_messages=deque(),
            url_cache_time=0,
            last_known_url="",
            driver=SimpleNamespace(current_url="https://page.example"),
        )
        flaresolverr_service._websocket_message_handler(session, {
            "method": "Network.webSocketFrameReceived",
            "params": {"url": "wss://socket.example", "response": {"payloadData": "hello"}},
        })

        message = session.websocket_messages[0]
        self.assertEqual((message.type, message.url, message.payload),
                         ("webSocketFrameReceived", "wss://socket.example", "hello"))


if __name__ == '__main__':
    unittest.main()
