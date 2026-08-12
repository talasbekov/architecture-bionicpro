import logging

import jwt
from fastapi import HTTPException, Request
from jwt import PyJWKClient

from .config import settings

log = logging.getLogger(__name__)

_jwk_client = PyJWKClient(settings.jwks_url, cache_keys=True)


class CurrentUser:
    def __init__(self, claims: dict) -> None:
        self.claims = claims
        self.sub = claims.get("sub")
        self.username = claims.get("preferred_username")
        self.roles = claims.get("realm_access", {}).get("roles", [])

    @property
    def user_id(self) -> str:
        # витрина построена по preferred_username, он же user_id в CRM
        return self.username or self.sub


def _decode(token: str) -> dict:
    signing_key = _jwk_client.get_signing_key_from_jwt(token)
    options = {"verify_aud": bool(settings.verify_audience)}
    return jwt.decode(
        token,
        signing_key.key,
        algorithms=["RS256"],
        audience=settings.verify_audience or None,
        options=options,
    )


async def get_current_user(request: Request) -> CurrentUser:
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Нужен access_token")

    token = header.split(" ", 1)[1]
    try:
        claims = _decode(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Срок действия токена истёк")
    except Exception as exc:
        log.warning("Токен не прошёл проверку: %s", exc)
        raise HTTPException(status_code=401, detail="Токен недействителен")

    if claims.get("iss") not in settings.expected_issuers:
        raise HTTPException(status_code=401, detail="Неизвестный издатель токена")

    user = CurrentUser(claims)
    if settings.required_role and settings.required_role not in user.roles:
        raise HTTPException(status_code=403, detail="Недостаточно прав для получения отчёта")
    return user
