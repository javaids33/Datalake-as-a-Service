"""
Auth client stub — wired into the sidecar but no-op until auth sidecar ships.
Provides the interface that other modules depend on for auth context.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("dlsidecar.auth.client")


class AuthClient:
    """Stub auth client. Will integrate with the auth sidecar when it ships."""

    def __init__(self):
        self._principal: str | None = None

    async def authenticate(self, token: str | None = None) -> str | None:
        """Authenticate and return the principal. Stub: returns decoded sub claim."""
        # When auth sidecar ships, this will call it for validation
        logger.debug("Auth client stub: no-op authenticate")
        return self._principal

    def set_principal(self, principal: str) -> None:
        self._principal = principal

    @property
    def principal(self) -> str | None:
        return self._principal
