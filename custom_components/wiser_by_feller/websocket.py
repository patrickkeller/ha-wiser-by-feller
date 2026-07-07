"""WebSocket wrapper that disables keepalive pings for old µGateway firmware.

Local fork patch. The `websockets` client sends a keepalive ping every ~20s by
default. µGateway v1 (Gen A / API v5, firmware 5.x) does not answer those pings,
so the client tears the connection down with a "keepalive ping timeout" every
~40s, producing an endless reconnect churn that hammers the weak gateway.

Passing ``ping_interval=None`` keeps a single connection open and lets the
gateway push load updates as designed (the coordinator's 30s polling remains the
safety net). We also reset the reconnect error counter whenever a message is
received, so a healthy connection never accumulates its way to the library's
permanent give-up after 10 drops.

The ``connect()`` body mirrors ``aiowiserbyfeller.Websocket.connect`` (pinned at
2.2.1); the only functional change is the added ``ping_interval=None``.
"""

from __future__ import annotations

from aiowiserbyfeller import Websocket
import websockets.client


class NoKeepalivePingWebsocket(Websocket):
    """Websocket that keeps the connection open without client keepalive pings."""

    async def connect(self) -> None:
        """Connect to the µGateway with keepalive pings disabled."""
        self._idle = False
        await self._watchdog.trigger()

        while True:
            try:
                async for ws in websockets.client.connect(
                    f"ws://{self._host}/api",
                    extra_headers={"Authorization": f"Bearer {self._token}"},
                    ping_interval=None,
                ):
                    try:
                        async for message in ws:
                            await self.on_message(message)
                    except websockets.ConnectionClosed:
                        self._errcount += 1
                        if self._errcount > 10:
                            self._logger.error(
                                "µGateway websocket connection closed 10 times. "
                                "Exiting connection..."
                            )
                            break

                        self._logger.warning(
                            "µGateway websocket connection closed. Reconnecting..."
                        )
                        continue
                    except (websockets.WebSocketException, ValueError) as e:
                        self.on_error(e)

                self._idle = True
                break

            except (websockets.WebSocketException, ValueError) as e:
                self.on_error(e)
                break

    async def on_message(self, message) -> None:
        """Handle a message and mark the connection healthy (reset drop counter)."""
        self._errcount = 0
        await super().on_message(message)
