import json
import logging
import secrets
import time
from typing import Optional

import redis.asyncio as aioredis
from cryptography.fernet import Fernet

from .config import settings

log = logging.getLogger(__name__)

SESSION_PREFIX = "session:"
FLOW_PREFIX = "login_flow:"


def _build_cipher() -> Fernet:
    key = settings.encryption_key
    if not key:
        key = Fernet.generate_key().decode()
        log.warning(
            "SESSION_ENCRYPTION_KEY не задан, сгенерирован временный ключ. "
            "После перезапуска все сессии станут недействительными."
        )
    return Fernet(key.encode() if isinstance(key, str) else key)


class SessionStore:
    """Сессии в Redis. Токены Keycloak лежат только здесь и только в зашифрованном виде."""

    def __init__(self) -> None:
        self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        self._cipher = _build_cipher()

    async def close(self) -> None:
        await self._redis.aclose()

    async def ping(self) -> bool:
        try:
            return bool(await self._redis.ping())
        except Exception:
            return False

    # ---- вспомогательный этап логина (state + code_verifier для PKCE) ----

    async def save_login_flow(self, state: str, payload: dict) -> None:
        await self._redis.setex(
            FLOW_PREFIX + state, settings.login_flow_ttl, json.dumps(payload)
        )

    async def pop_login_flow(self, state: str) -> Optional[dict]:
        key = FLOW_PREFIX + state
        raw = await self._redis.get(key)
        if raw is None:
            return None
        # state одноразовый
        await self._redis.delete(key)
        return json.loads(raw)

    # ---- сессии ----

    @staticmethod
    def new_session_id() -> str:
        return secrets.token_urlsafe(32)

    def _encrypt(self, data: dict) -> str:
        return self._cipher.encrypt(json.dumps(data).encode()).decode()

    def _decrypt(self, blob: str) -> dict:
        return json.loads(self._cipher.decrypt(blob.encode()).decode())

    async def create(self, data: dict) -> str:
        session_id = self.new_session_id()
        await self._redis.setex(
            SESSION_PREFIX + session_id, settings.session_ttl, self._encrypt(data)
        )
        return session_id

    async def get(self, session_id: str) -> Optional[dict]:
        if not session_id:
            return None
        raw = await self._redis.get(SESSION_PREFIX + session_id)
        if raw is None:
            return None
        try:
            return self._decrypt(raw)
        except Exception:
            log.warning("Не удалось расшифровать сессию, удаляем её")
            await self.delete(session_id)
            return None

    async def update(self, session_id: str, data: dict) -> None:
        ttl = await self._redis.ttl(SESSION_PREFIX + session_id)
        if ttl is None or ttl < 0:
            ttl = settings.session_ttl
        await self._redis.setex(SESSION_PREFIX + session_id, ttl, self._encrypt(data))

    async def delete(self, session_id: str) -> None:
        await self._redis.delete(SESSION_PREFIX + session_id)

    async def rotate(self, old_session_id: str, data: dict) -> str:
        """Перепривязывает токены к новому session id. Защита от session fixation."""
        new_session_id = self.new_session_id()
        # срок жизни сессии считаем заново от момента активности пользователя
        await self._redis.setex(
            SESSION_PREFIX + new_session_id, settings.session_ttl, self._encrypt(data)
        )
        await self._redis.delete(SESSION_PREFIX + old_session_id)
        return new_session_id


def token_expired(session: dict, leeway: int = 10) -> bool:
    return time.time() >= session.get("access_expires_at", 0) - leeway
