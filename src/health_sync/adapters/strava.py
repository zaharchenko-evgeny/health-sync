"""Direct Strava API adapter."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx


@dataclass
class StravaTokens:
    access_token: str | None
    refresh_token: str
    expires_at: int = 0


class StravaClient:
    def __init__(
        self,
        *,
        client_id: str | None,
        client_secret: str | None,
        refresh_token: str | None,
        access_token: str | None = None,
        expires_at: int = 0,
        token_file: Path | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_file = token_file
        self.http_client = http_client
        file_tokens = self._load_token_file()
        self.tokens = StravaTokens(
            access_token=access_token or file_tokens.get("access_token"),
            refresh_token=refresh_token or file_tokens.get("refresh_token") or "",
            expires_at=expires_at or int(file_tokens.get("expires_at") or 0),
        )

    def _load_token_file(self) -> dict[str, Any]:
        if not self.token_file or not self.token_file.exists():
            return {}
        return json.loads(self.token_file.read_text())

    def _save_token_file(self) -> None:
        if not self.token_file:
            return
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        self.token_file.write_text(
            json.dumps(
                {
                    "access_token": self.tokens.access_token,
                    "refresh_token": self.tokens.refresh_token,
                    "expires_at": self.tokens.expires_at,
                },
                indent=2,
            )
        )
        self.token_file.chmod(0o600)

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        if self.http_client:
            return await self.http_client.request(method, url, **kwargs)
        async with httpx.AsyncClient(timeout=30) as client:
            return await client.request(method, url, **kwargs)

    async def ensure_access_token(self) -> str:
        if self.tokens.access_token and self.tokens.expires_at > int(time.time()) + 300:
            return self.tokens.access_token
        if not self.client_id or not self.client_secret or not self.tokens.refresh_token:
            raise ValueError("Strava client_id, client_secret, and refresh_token are required")

        response = await self._request(
            "POST",
            "https://www.strava.com/oauth/token",
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.tokens.refresh_token,
                "grant_type": "refresh_token",
            },
        )
        response.raise_for_status()
        data = response.json()
        self.tokens.access_token = data["access_token"]
        self.tokens.refresh_token = data.get("refresh_token", self.tokens.refresh_token)
        self.tokens.expires_at = int(data.get("expires_at") or 0)
        self._save_token_file()
        return self.tokens.access_token

    async def update_athlete_weight(self, weight_kg: float) -> dict[str, Any]:
        token = await self.ensure_access_token()
        response = await self._request(
            "PUT",
            "https://www.strava.com/api/v3/athlete",
            headers={"Authorization": f"Bearer {token}"},
            data={"weight": str(weight_kg)},
        )
        response.raise_for_status()
        return response.json()
