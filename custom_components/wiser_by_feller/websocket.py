"""WebSocket wrapper for the µGateway, tuned for the weak Gen A firmware.

Local fork patch. Problems with the upstream ``aiowiserbyfeller.Websocket`` on
µGateway v1 (Gen A / API v5, firmware 5.x), and what this subclass does:

1. The ``websockets`` client sends a keepalive ping every ~20s by default. The
   old firmware does not answer it, so the client tears the connection down with
   a "keepalive ping timeout" every ~40s. We pass ``ping_interval=None``.

2. Without pings, a connection whose peer vanished (gateway watchdog reboot,
   WLAN drop) is never noticed: HA only reads, so the half-open socket waits
   forever and push silently stops. Three independent detectors cover this:
   - TCP keepalive on the socket. Probes are answered by the gateway's WLAN
     module TCP stack, not by MicroPython, so they cost the gateway nothing;
     a rebooted gateway answers them with a reset.
   - The library watchdog (no message for 15 min) recycles the connection
     instead of only logging.
   - The coordinator restarts the connection when the gateway's uptime shows
     it rebooted after the connection was opened (see ``connected_since``).

3. ``Websocket.async_close()`` never assigns ``self._ws``, so it is a no-op. We
   track the background ``connect()`` task (cancel / restart) and the current
   connection (closed deterministically).

4. One malformed frame (``ValueError``) or one exception in a subscriber ended
   the whole connect task, i.e. push was gone until the next poll. Errors are
   now handled per message.

5. A closed connection was reopened immediately (up to 11 handshakes in a row
   against an already struggling gateway); a clean close even reconnected
   without limit. Reconnects now back off exponentially.

The ``connect()`` loop is based on ``aiowiserbyfeller.Websocket.connect`` (pinned
at 2.2.1) — re-check it on a library bump.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
import contextlib
import json
import socket
import time

from aiowiserbyfeller import Websocket
import websockets.client

# The gateway answers trivial HTTP calls in 6-8s under load; the library's
# default 10s handshake timeout is too tight.
OPEN_TIMEOUT = 30
# Do not wait long for a close handshake the gateway may never answer.
CLOSE_TIMEOUT = 5
# Exponential reconnect backoff after a dropped connection.
RECONNECT_DELAY_MIN = 5
RECONNECT_DELAY_MAX = 120
# Consecutive drops without a single message before the loop gives up; the
# coordinator restarts it on its next poll.
MAX_CONSECUTIVE_DROPS = 10
# TCP keepalive: first probe after 2 min idle, then every 30s, dead after 4
# unanswered probes (a rebooted gateway resets the connection on the first).
TCP_KEEPALIVE_IDLE = 120
TCP_KEEPALIVE_INTERVAL = 30
TCP_KEEPALIVE_COUNT = 4


def enable_tcp_keepalive(sock: socket.socket | None) -> None:
    """Enable TCP keepalive probes on the WebSocket's socket (best effort)."""
    if sock is None:
        return
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    # TCP_KEEPIDLE on Linux (HA OS), TCP_KEEPALIVE on macOS.
    idle_opt = getattr(socket, "TCP_KEEPIDLE", None) or getattr(
        socket, "TCP_KEEPALIVE", None
    )
    for opt, value in (
        (idle_opt, TCP_KEEPALIVE_IDLE),
        (getattr(socket, "TCP_KEEPINTVL", None), TCP_KEEPALIVE_INTERVAL),
        (getattr(socket, "TCP_KEEPCNT", None), TCP_KEEPALIVE_COUNT),
    ):
        if opt is not None:
            sock.setsockopt(socket.IPPROTO_TCP, opt, value)


class GatewayWebsocket(Websocket):
    """WebSocket to the µGateway that detects dead connections and can be restarted."""

    def __init__(
        self,
        *args,
        create_task: Callable[[Coroutine], asyncio.Task] = asyncio.create_task,
        **kwargs,
    ) -> None:
        """Track the background connect() task so it can be cancelled/restarted.

        ``create_task`` lets Home Assistant own the task (see coordinator), so
        it is tracked and cancelled on shutdown like any HA background task.
        """
        super().__init__(*args, **kwargs)
        self._create_task = create_task
        self._task: asyncio.Task[None] | None = None
        self._connected_since: float | None = None
        # Serializes close/restart: the watchdog, the coordinator's poll and an
        # unload may all act at once; without this two connect loops (or one
        # after unload) could survive and occupy the gateway's few sockets.
        self._lifecycle_lock = asyncio.Lock()
        self._closed = False

    @property
    def connected_since(self) -> float | None:
        """Monotonic time the current connection was opened, None if not connected."""
        return self._connected_since

    def init(self) -> None:
        """Start the background connect loop, tracking the task."""
        self._closed = False
        self._task = self._create_task(self.connect())

    def is_running(self) -> bool:
        """Return True while the background connect() task is alive."""
        return self._task is not None and not self._task.done()

    async def async_close(self) -> None:
        """Actually stop the connection (upstream async_close is a no-op)."""
        async with self._lifecycle_lock:
            self._closed = True
            await self._async_stop_task()
            await super().async_close()

    async def async_restart(self) -> None:
        """Drop the current connection (if any) and connect again.

        Does nothing once closed, so a restart racing an unload can't reconnect.
        """
        async with self._lifecycle_lock:
            if self._closed:
                return
            await self._async_stop_task()
            self.reset_error_count()
            self.init()

    async def _async_stop_task(self) -> None:
        """Cancel the connect loop and wait until its connection is closed."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def connect(self) -> None:
        """Connect to the µGateway and process messages, reconnecting on drops."""
        self._idle = False
        await self._watchdog.trigger()

        connections = websockets.client.connect(
            f"ws://{self._host}/api",
            extra_headers={"Authorization": f"Bearer {self._token}"},
            ping_interval=None,
            open_timeout=OPEN_TIMEOUT,
            close_timeout=CLOSE_TIMEOUT,
        ).__aiter__()

        try:
            # aclosing() closes the current connection deterministically when
            # this task is cancelled, instead of leaving the socket to the GC.
            async with contextlib.aclosing(connections):
                async for ws in connections:
                    await self._process_connection(ws)
                    self._errcount += 1
                    if self._errcount > MAX_CONSECUTIVE_DROPS:
                        self._logger.error(
                            "µGateway websocket connection closed %s times in a row. "
                            "Exiting connection...",
                            MAX_CONSECUTIVE_DROPS,
                        )
                        break

                    delay = min(
                        RECONNECT_DELAY_MIN * 2 ** (self._errcount - 1),
                        RECONNECT_DELAY_MAX,
                    )
                    self._logger.warning(
                        "µGateway websocket connection closed. Reconnecting in %ss...",
                        delay,
                    )
                    await asyncio.sleep(delay)
        except Exception:
            # Connection failures are retried inside the websockets iterator;
            # anything reaching here is unexpected. Log it and end the task —
            # the coordinator restarts it on its next poll.
            self._logger.exception("µGateway websocket failed")
        finally:
            self._idle = True
            self._connected_since = None

    async def _process_connection(self, ws) -> None:
        """Read messages from one connection until it closes."""
        self._ws = ws
        self._connected_since = time.monotonic()
        try:
            enable_tcp_keepalive(ws.transport.get_extra_info("socket"))
        except (AttributeError, OSError) as err:
            self._logger.debug("Could not enable TCP keepalive: %s", err)

        try:
            async for message in ws:
                await self.on_message(message)
        except websockets.ConnectionClosed:
            pass  # Clean closes end the loop silently; both are a drop.
        finally:
            self._ws = None
            self._connected_since = None

    async def on_message(self, message) -> None:
        """Dispatch one message; a bad message or subscriber must not kill the loop.

        Mirrors ``aiowiserbyfeller.Websocket.on_message`` with per-message error
        handling. Any message marks the connection healthy (resets drop counter).
        """
        self._errcount = 0
        await self._watchdog.trigger()

        try:
            data = json.loads(message)
        except ValueError:
            self._logger.warning(
                "Ignoring malformed websocket message from µGateway: %.200r", message
            )
            return

        for fn in self._subscribers:
            try:
                fn(data)
            except Exception:  # noqa: PERF203
                self._logger.exception("Error handling websocket message: %s", data)
        for fn in self._async_subscribers:
            try:
                await fn(data)
            except Exception:  # noqa: PERF203
                self._logger.exception("Error handling websocket message: %s", data)

    async def on_watchdog_timeout(self) -> None:
        """No message for 15 min: the connection may be dead — recycle it.

        Upstream only logs here. A fresh handshake every 15 min of silence is
        negligible load and bounds how long a silently dead connection survives.
        """
        if not self.is_running():
            return
        self._logger.info(
            "No websocket message from µGateway for 15 min; reconnecting."
        )
        await self.async_restart()
