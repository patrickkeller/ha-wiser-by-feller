"""Tests for the request-serializing Auth used to spare the µGateway's sockets."""

import asyncio
from unittest.mock import MagicMock, patch

from aiowiserbyfeller import Auth
import pytest

from custom_components.wiser_by_feller import gateway_auth
from custom_components.wiser_by_feller.gateway_auth import SerializedAuth


async def test_requests_are_sent_one_at_a_time():
    """Concurrent callers (e.g. a scene switching many loads) never overlap."""
    in_flight = 0
    max_in_flight = 0

    async def fake_request(self, method, path, **kwargs):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return path

    auth = SerializedAuth(MagicMock(), "host", token="token")
    with patch.object(Auth, "request", fake_request):
        results = await asyncio.gather(
            *(auth.request("put", f"loads/{i}/ctrl") for i in range(8))
        )

    assert max_in_flight == 1
    assert results == [f"loads/{i}/ctrl" for i in range(8)]


async def test_failed_request_releases_the_slot():
    """An exception must not leave the limiter locked."""

    async def failing_request(self, method, path, **kwargs):
        raise RuntimeError("boom")

    auth = SerializedAuth(MagicMock(), "host", token="token")
    with patch.object(Auth, "request", failing_request):
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await auth.request("get", "loads/state")

    assert not auth._lock.locked()


async def test_hung_request_times_out_and_releases_the_lock():
    """A request the gateway never answers must not block all others."""

    async def hung_request(self, method, path, **kwargs):
        await asyncio.Event().wait()

    auth = SerializedAuth(MagicMock(), "host", token="token")
    with (
        patch.object(gateway_auth, "REQUEST_TIMEOUT", 0.01),
        patch.object(Auth, "request", hung_request),
        pytest.raises(TimeoutError),
    ):
        await auth.request("get", "loads/state")

    assert not auth._lock.locked()
