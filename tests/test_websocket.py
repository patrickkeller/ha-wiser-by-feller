"""Tests for the keepalive-ping-free WebSocket used on old µGateway firmware."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from custom_components.wiser_by_feller.websocket import NoKeepalivePingWebsocket


def _make_ws():
    ws = NoKeepalivePingWebsocket("host", "token", MagicMock())
    # Avoid scheduling the real 900s watchdog timer during tests.
    ws._watchdog = MagicMock()
    ws._watchdog.trigger = AsyncMock()
    ws._watchdog.cancel = MagicMock()
    return ws


async def test_connect_disables_keepalive_pings():
    """connect() must pass ping_interval=None so old firmware isn't ping-timed-out."""
    captured: dict = {}

    class _EmptyConnect:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    ws = _make_ws()
    with patch(
        "custom_components.wiser_by_feller.websocket.websockets.client.connect",
        _EmptyConnect,
    ):
        await ws.connect()

    assert "ping_interval" in captured
    assert captured["ping_interval"] is None
    assert ws.is_idle() is True


async def test_on_message_resets_error_count():
    """A received message marks the connection healthy and resets the drop counter."""
    ws = _make_ws()
    ws._errcount = 7

    await ws.on_message('{"load": {"id": 1, "state": {"bri": 100}}}')

    assert ws._errcount == 0


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
