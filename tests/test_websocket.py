"""Tests for the WebSocket to the µGateway (dead-connection detection, restarts)."""

import asyncio
import socket
from unittest.mock import AsyncMock, MagicMock, patch

import websockets

from custom_components.wiser_by_feller import websocket as ws_module
from custom_components.wiser_by_feller.websocket import (
    MAX_CONSECUTIVE_DROPS,
    GatewayWebsocket,
    enable_tcp_keepalive,
)

CONNECT = "custom_components.wiser_by_feller.websocket.websockets.client.connect"


def _make_ws():
    ws = GatewayWebsocket("host", "token", MagicMock())
    # Avoid scheduling the real 900s watchdog timer during tests.
    ws._watchdog = MagicMock()
    ws._watchdog.trigger = AsyncMock()
    ws._watchdog.cancel = MagicMock()
    return ws


class _FakeConnection:
    """A websocket connection yielding the given messages, then closing."""

    def __init__(self, messages=(), error=None):
        self._messages = list(messages)
        self._error = error
        self.transport = MagicMock()
        self.transport.get_extra_info.return_value = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._messages:
            return self._messages.pop(0)
        if self._error is not None:
            raise self._error
        raise StopAsyncIteration


class _FakeConnect:
    """Stand-in for websockets.client.connect: yields the given connections."""

    instances: list = []

    def __init__(self, *args, connections=(), **kwargs):
        self.kwargs = kwargs
        self._connections = list(connections)
        self.closed = False
        _FakeConnect.instances.append(self)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._connections:
            return self._connections.pop(0)
        raise StopAsyncIteration

    async def aclose(self):
        self.closed = True


def _fake_connect(*connections):
    _FakeConnect.instances = []
    return lambda *args, **kwargs: _FakeConnect(
        *args, connections=connections, **kwargs
    )


async def test_connect_disables_keepalive_pings_and_uses_long_open_timeout():
    """connect() must not ping (old firmware never answers) and allow slow handshakes."""
    ws = _make_ws()
    with patch(CONNECT, _fake_connect()):
        await ws.connect()

    kwargs = _FakeConnect.instances[0].kwargs
    assert kwargs["ping_interval"] is None
    assert kwargs["open_timeout"] == ws_module.OPEN_TIMEOUT
    assert ws.is_idle() is True
    assert _FakeConnect.instances[0].closed is True


async def test_reconnect_backs_off_exponentially():
    """Drops are not reconnected immediately (that stormed the weak gateway)."""
    ws = _make_ws()
    closed = websockets.ConnectionClosedError(None, None)
    with (
        patch(CONNECT, _fake_connect(_FakeConnection(error=closed), _FakeConnection())),
        patch.object(ws_module.asyncio, "sleep", AsyncMock()) as sleep,
    ):
        await ws.connect()

    assert [c.args[0] for c in sleep.await_args_list] == [5, 10]


async def test_messages_reset_the_backoff():
    """A connection that delivered messages reconnects after the minimum delay."""
    ws = _make_ws()
    ws._errcount = 5
    with (
        patch(CONNECT, _fake_connect(_FakeConnection(messages=['{"x": 1}']))),
        patch.object(ws_module.asyncio, "sleep", AsyncMock()) as sleep,
    ):
        await ws.connect()

    sleep.assert_awaited_once_with(ws_module.RECONNECT_DELAY_MIN)


async def test_gives_up_after_too_many_consecutive_drops():
    """After MAX_CONSECUTIVE_DROPS silent drops the loop ends (coordinator restarts it)."""
    ws = _make_ws()
    connections = [_FakeConnection() for _ in range(MAX_CONSECUTIVE_DROPS + 5)]
    with (
        patch(CONNECT, _fake_connect(*connections)),
        patch.object(ws_module.asyncio, "sleep", AsyncMock()) as sleep,
    ):
        await ws.connect()

    assert sleep.await_count == MAX_CONSECUTIVE_DROPS
    assert _FakeConnect.instances[0].closed is True


async def test_connected_since_tracks_the_open_connection():
    """connected_since is set while a connection is open and cleared afterwards."""
    ws = _make_ws()
    seen = []
    ws.subscribe(lambda data: seen.append(ws.connected_since))
    with (
        patch(CONNECT, _fake_connect(_FakeConnection(messages=['{"x": 1}']))),
        patch.object(ws_module.asyncio, "sleep", AsyncMock()),
    ):
        await ws.connect()

    assert seen[0] is not None
    assert ws.connected_since is None


async def test_on_message_resets_error_count():
    """A received message marks the connection healthy and resets the drop counter."""
    ws = _make_ws()
    ws._errcount = 7

    await ws.on_message('{"load": {"id": 1, "state": {"bri": 100}}}')

    assert ws._errcount == 0


async def test_malformed_message_is_ignored():
    """A garbled frame is logged and skipped instead of ending the connection."""
    ws = _make_ws()
    callback = MagicMock()
    ws.subscribe(callback)

    await ws.on_message('{"load": {"id": 1, "sta')

    callback.assert_not_called()


async def test_subscriber_error_does_not_stop_dispatch():
    """An exception in one subscriber neither propagates nor skips the others."""
    ws = _make_ws()
    failing = MagicMock(side_effect=KeyError("state"))
    healthy = MagicMock()
    ws.subscribe(failing)
    ws.subscribe(healthy)

    await ws.on_message('{"load": {"id": 1}}')

    healthy.assert_called_once_with({"load": {"id": 1}})


async def test_init_is_running_and_async_close_track_the_task():
    """init() starts a tracked task; async_close() cancels it (upstream can't)."""
    ws = _make_ws()
    assert ws.is_running() is False

    started = asyncio.Event()

    async def _blocking_connect():
        started.set()
        await asyncio.Event().wait()  # run until cancelled

    with patch.object(ws, "connect", _blocking_connect):
        ws.init()
        await started.wait()
        assert ws.is_running() is True

        await ws.async_close()
        assert ws.is_running() is False
        assert ws._task is None


async def test_watchdog_timeout_restarts_a_running_connection():
    """15 min without a message: recycle the (possibly dead) connection."""
    ws = _make_ws()
    ws.is_running = MagicMock(return_value=True)
    ws.async_restart = AsyncMock()

    await ws.on_watchdog_timeout()

    ws.async_restart.assert_awaited_once()


async def test_watchdog_timeout_ignored_when_stopped():
    """A stopped WebSocket (e.g. after unload) is not resurrected by the watchdog."""
    ws = _make_ws()
    ws.async_restart = AsyncMock()

    await ws.on_watchdog_timeout()

    ws.async_restart.assert_not_called()


async def test_async_restart_closes_and_starts_again():
    """async_restart() stops the current task and starts a fresh connect loop."""
    ws = _make_ws()
    ws._errcount = 11
    ws._async_stop_task = AsyncMock()
    ws.init = MagicMock()

    await ws.async_restart()

    ws._async_stop_task.assert_awaited_once()
    ws.init.assert_called_once()
    assert ws._errcount == 0


def test_enable_tcp_keepalive_sets_socket_options():
    """TCP keepalive is enabled so a vanished gateway is detected."""
    sock = MagicMock()

    enable_tcp_keepalive(sock)

    sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    assert sock.setsockopt.call_count >= 2  # plus the idle/interval/count tuning


def test_enable_tcp_keepalive_tolerates_missing_socket():
    """No socket available (e.g. mocked transport) is a no-op."""
    enable_tcp_keepalive(None)


async def test_restart_after_close_does_not_reconnect():
    """A restart racing an unload must not bring the connection back."""
    ws = _make_ws()
    ws.init = MagicMock()

    await ws.async_close()
    await ws.async_restart()

    ws.init.assert_not_called()


async def test_concurrent_restarts_leave_a_single_connect_loop():
    """Watchdog and poll restarting at once must not leave two connect loops."""
    ws = _make_ws()
    started = []

    async def _blocking_connect():
        started.append(asyncio.current_task())
        await asyncio.Event().wait()

    with patch.object(ws, "connect", _blocking_connect):
        ws.init()
        await asyncio.sleep(0)
        await asyncio.gather(ws.async_restart(), ws.async_restart())
        await asyncio.sleep(0)

        alive = [task for task in started if not task.done()]
        assert alive == [ws._task]

        await ws.async_close()


async def test_connect_loop_runs_in_the_given_task_factory():
    """The connect loop is started via the injected factory (HA background task)."""
    created = []

    def factory(coro):
        task = asyncio.get_running_loop().create_task(coro)
        created.append(task)
        return task

    ws = GatewayWebsocket("host", "token", MagicMock(), create_task=factory)
    ws._watchdog = MagicMock()
    ws._watchdog.trigger = AsyncMock()

    with patch.object(ws, "connect", AsyncMock()):
        ws.init()
        await asyncio.sleep(0)

    assert created == [ws._task]
