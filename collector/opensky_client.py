"""
OpenSky Network client — OAuth2 client-credentials auth + state vector fetch.

Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 10 (Phase 2)
and section 17 (OpenSky Integration Notes & References).

Per the guide: "Use the official documentation and current access policy
as the source of truth while implementing the collector." Verified against
https://openskynetwork.github.io/opensky-api/rest.html and
https://opensky-network.org/about/faq (checked 2026-08-24):

- Basic auth (username/password) is deprecated; OAuth2 client-credentials
  is now required/recommended for all programmatic access.
- Token endpoint:
  https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token
- grant_type=client_credentials, client_id, client_secret -> access_token,
  expires_in (~1800s / 30 min).
- Without credentials, requests are anonymous with reduced rate limits.
- Authenticated OpenSky users get ~4000 API credits/day.
- GET https://opensky-network.org/api/states/all with
  "Authorization: Bearer <token>". Optional filters: time, icao24,
  lamin/lomin/lamax/lomax (bounding box).
- 401 -> token expired, refresh and retry once.
- 429 -> rate limited, back off (Retry-After header if present).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger("flightpulse.collector.opensky")

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"

TOKEN_REFRESH_MARGIN_SECONDS = 30


class TokenManager:
    """Handles OAuth2 client-credentials token retrieval and refresh."""

    def __init__(self, client_id: Optional[str], client_secret: Optional[str]):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

    @property
    def is_authenticated(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def get_token(self) -> Optional[str]:
        if not self.is_authenticated:
            return None  # anonymous access
        if self._token and self._expires_at and datetime.utcnow() < self._expires_at:
            return self._token
        return self._refresh()

    def _refresh(self) -> str:
        logger.info("Requesting new OpenSky OAuth2 token")
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self._expires_at = datetime.utcnow() + timedelta(
            seconds=expires_in - TOKEN_REFRESH_MARGIN_SECONDS
        )
        logger.info("OpenSky token acquired, expires_in=%ss", expires_in)
        return self._token

    def auth_headers(self) -> dict:
        token = self.get_token()
        if not token:
            return {}
        return {"Authorization": f"Bearer {token}"}


class OpenSkyClient:
    """Minimal REST client for OpenSky's /states/all endpoint."""

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        base_url: str = STATES_URL,
        request_timeout: int = 15,
    ):
        self.token_manager = TokenManager(client_id, client_secret)
        self.base_url = base_url
        self.request_timeout = request_timeout

    def get_states(
        self,
        icao24: Optional[list[str]] = None,
        bbox: Optional[tuple[float, float, float, float]] = None,
        max_retries: int = 1,
    ) -> Optional[dict]:
        """
        Fetch current state vectors. Returns the raw OpenSky JSON response
        (dict with 'time' and 'states'), or None on unrecoverable failure.

        bbox, if given, is (lamin, lomin, lamax, lomax) per the REST docs.
        """
        params = {}
        if icao24:
            params["icao24"] = icao24
        if bbox:
            lamin, lomin, lamax, lomax = bbox
            params.update(lamin=lamin, lomin=lomin, lamax=lamax, lomax=lomax)

        for attempt in range(max_retries + 1):
            headers = self.token_manager.auth_headers()
            try:
                resp = requests.get(
                    self.base_url,
                    params=params,
                    headers=headers,
                    timeout=self.request_timeout,
                )
            except requests.RequestException as exc:
                logger.error("OpenSky request failed: %s", exc)
                return None

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 401 and attempt < max_retries:
                logger.warning("OpenSky token expired (401), refreshing and retrying")
                self.token_manager._token = None  # force refresh
                continue

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                logger.warning(
                    "OpenSky rate limit hit (429). Retry-After=%ss", retry_after
                )
                return None

            logger.error(
                "OpenSky request failed: status=%s body=%s",
                resp.status_code,
                resp.text[:500],
            )
            return None

        return None
