"""WebSocket wrapper for the µGateway, tuned for the weak Gen A firmware.

Local fork patch. Two problems with the upstream ``aiowiserbyfeller.Websocket``
on µGateway v1 (Gen A / API v5, firmware 5.x):

1. The ``websockets`` client sends a keepalive ping every ~20s by default. The
   old firmware does not answer it, so the client tears the connection down with
   a "keepalive ping timeout" every ~40s. We pass ``ping_interval=None`` to keep
   a single connection open; the coordinator's polling remains the safety net.

2. ``Websocket.async_close()`` never assigns ``self._ws``, so it is a no-op — a
   dead or stale connection can neither be torn down nor restarted. We track the
   background ``connect()`` task so it can be cancelled (``async_close``) and
   restarted (``init``). This lets the coordinator keep the WebSocket — the
   primary update source on this integration — reliably alive.

The ``connect()`` body mirrors ``aiowiserbyfeller.Websocket.connect`` (pinned at
2.2.1); the only functional change is the added ``ping_interval=None``.
"""

from __future__ import annotations

import asyncio
import contextlib

from aiowiserbyfeller import Websocket
import websockets.client


class NoKeepalivePingWebsocket(Websocket):
    """WebSocket that stays open without keepalive pings and can be restarted."""

    def __init__(self, *args, **kwargs) -> None:
        """Track the background connect() task so it can be cancelled/restarted."""
        super().__init__(*args, **kwargs)
        self._task: asyncio.Task[None] | None = None

    def init(self) -> None:
        """Start (or restart) the background connect loop, tracking the task."""
        self._task = asyncio.create_task(self.connect())  # noqa: RUF006

    def is_running(self) -> bool:
        """Return True while the background connect() task is alive."""
        return self._task is not None and not self._task.done()

    async def async_close(self) -> None:
        """Actually stop the connection (upstream async_close is a no-op)."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await super().async_close()

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
