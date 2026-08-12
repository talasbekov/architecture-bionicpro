import base64
import hashlib
import secrets
from typing import Optional
from urllib.parse import urlencode

import httpx

from .config import settings


def generate_pkce_pair() -> tuple[str, str]:
    """code_verifier и code_challenge по методу S256 (RFC 7636)."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).decode().rstrip("=")
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode().rstrip("=")
    return verifier, challenge


def build_authorization_url(state: str, code_challenge: str, nonce: str) -> str:
    params = {
        "client_id": settings.client_id,
        "response_type": "code",
        "redirect_uri": settings.redirect_uri,
        "scope": settings.scope,
        "state": state,
        "nonce": nonce,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{settings.authorization_endpoint}?{urlencode(params)}"


class KeycloakClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(timeout=10.0)

    async def close(self) -> None:
        await self._client.aclose()

    def _with_credentials(self, data: dict) -> dict:
        data["client_id"] = settings.client_id
        if settings.client_secret:
            data["client_secret"] = settings.client_secret
        return data

    async def exchange_code(self, code: str, code_verifier: str) -> dict:
        data = self._with_credentials(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": settings.redirect_uri,
                "code_verifier": code_verifier,
            }
        )
        response = await self._client.post(settings.token_endpoint, data=data)
        response.raise_for_status()
        return response.json()

    async def refresh(self, refresh_token: str) -> dict:
        data = self._with_credentials(
            {"grant_type": "refresh_token", "refresh_token": refresh_token}
        )
        response = await self._client.post(settings.token_endpoint, data=data)
        response.raise_for_status()
        return response.json()

    async def logout(self, refresh_token: Optional[str]) -> None:
        if not refresh_token:
            return
        data = self._with_credentials({"refresh_token": refresh_token})
        try:
            await self._client.post(settings.logout_endpoint, data=data)
        except httpx.HTTPError:
            # выход на стороне Keycloak не должен ломать выход из нашего приложения
            pass

    async def userinfo(self, access_token: str) -> dict:
        response = await self._client.get(
            settings.userinfo_endpoint,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        return response.json()
