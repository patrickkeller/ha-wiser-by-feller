"""Authenticated HTTP access to the µGateway, one request at a time.

Local fork patch. µGateway v1 (Gen A, firmware 5.x) has 9 sockets in total and
crashes (``AT_TIMEOUT`` watchdog reboot) under socket pressure. Without a limit,
a Home Assistant scene, group or automation switching several Wiser loads opens
one HTTP connection per load at the same time, on top of the WebSocket and the
poll. The gateway processes requests one after another anyway, so parallel
requests only queue there while holding its sockets and RAM.

Every request (polls, commands, services) goes through ``Auth.request`` of the
single ``Auth`` instance shared by the API and all Load/Job/SystemFlag objects,
so serializing it here caps HA at one HTTP connection plus the WebSocket.
"""

from __future__ import annotations

import asyncio

from aiowiserbyfeller import Auth

# A single request may take 5-17s on µGateway v1 (see coordinator). Bound it,
# so a hung request can't hold the lock and block every command and poll for
# aiohttp's 5-minute default timeout.
REQUEST_TIMEOUT = 30


class SerializedAuth(Auth):
    """``Auth`` that sends one request at a time, each bounded by REQUEST_TIMEOUT."""

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the request lock."""
        super().__init__(*args, **kwargs)
        self._lock = asyncio.Lock()

    async def request(self, method: str, path: str, **kwargs):
        """Send a request to the API once no other request is in flight."""
        async with self._lock, asyncio.timeout(REQUEST_TIMEOUT):
            return await super().request(method, path, **kwargs)
