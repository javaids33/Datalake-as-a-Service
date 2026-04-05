"""
JWT manager — reads token from DLS_JWT_TOKEN_PATH, decodes without verification
to get expiry, runs a background refresh loop that re-reads the file before expiry.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import jwt as pyjwt

from dlsidecar.config import settings
from dlsidecar.governance.metrics import JWT_EXPIRY_SECONDS

logger = logging.getLogger("dlsidecar.auth.jwt")


class JWTManager:
    """Manages JWT token lifecycle: read, decode, refresh."""

    def __init__(self, token_path: str | None = None, refresh_before_expiry: int | None = None):
        self.token_path = Path(token_path or settings.jwt_token_path)
        self.refresh_before_expiry = refresh_before_expiry or settings.jwt_refresh_before_expiry_seconds
        self._token: str | None = None
        self._claims: dict | None = None
        self._expiry: float = 0.0
        self._refresh_task: asyncio.Task | None = None
        self._callbacks: list = []

    @property
    def token(self) -> str | None:
        return self._token

    @property
    def claims(self) -> dict | None:
        return self._claims

    @property
    def subject(self) -> str | None:
        if self._claims:
            return self._claims.get("sub")
        return None

    @property
    def issuer(self) -> str | None:
        if self._claims:
            return self._claims.get("iss")
        return None

    @property
    def seconds_until_expiry(self) -> float:
        if self._expiry == 0:
            return 0
        return max(0, self._expiry - time.time())

    @property
    def is_expired(self) -> bool:
        return self._expiry > 0 and time.time() >= self._expiry

    def on_refresh(self, callback) -> None:
        """Register a callback to be called when token is refreshed."""
        self._callbacks.append(callback)

    def load(self) -> str | None:
        """Read token from file and decode claims. Returns token or None."""
        if not self.token_path.exists():
            logger.warning("JWT token file not found: %s", self.token_path)
            return None

        raw = self.token_path.read_text().strip()
        if not raw:
            logger.warning("JWT token file is empty: %s", self.token_path)
            return None

        try:
            claims = pyjwt.decode(raw, options={"verify_signature": False})
        except pyjwt.DecodeError as exc:
            logger.error("Failed to decode JWT: %s", exc)
            return None

        self._token = raw
        self._claims = claims
        self._expiry = claims.get("exp", 0)

        JWT_EXPIRY_SECONDS.set(self.seconds_until_expiry)
        logger.info(
            "JWT loaded: sub=%s iss=%s expires_in=%.0fs",
            self.subject,
            self.issuer,
            self.seconds_until_expiry,
        )
        return raw

    async def start_refresh_loop(self) -> None:
        """Start background task that re-reads the token before expiry."""
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass

    async def _refresh_loop(self) -> None:
        while True:
            wait = self.seconds_until_expiry - self.refresh_before_expiry
            if wait > 0:
                await asyncio.sleep(wait)
            else:
                await asyncio.sleep(30)

            old_token = self._token
            self.load()
            JWT_EXPIRY_SECONDS.set(self.seconds_until_expiry)

            if self._token and self._token != old_token:
                logger.info("JWT refreshed, notifying %d callbacks", len(self._callbacks))
                for cb in self._callbacks:
                    try:
                        result = cb(self._token)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        logger.exception("JWT refresh callback failed")
