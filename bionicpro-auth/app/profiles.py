import json
import logging
import os
from typing import Optional

import asyncpg

log = logging.getLogger(__name__)

CRM_DSN = os.getenv(
    "CRM_DSN", "postgresql://crm_user:crm_password@crm_db:5432/crm"
)


class ProfileStore:
    """Профили пользователей, полученные от внешних удостоверяющих служб (Яндекс ID и т.д.)."""

    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> Optional[asyncpg.Pool]:
        """Подключение ленивое: на старте CRM может быть ещё не поднята,
        поэтому пробуем снова при каждом обращении, а не один раз при запуске."""
        if self._pool is not None:
            return self._pool
        try:
            self._pool = await asyncpg.create_pool(CRM_DSN, min_size=1, max_size=5)
        except Exception as exc:
            # сервис аутентификации не должен падать, если CRM недоступна
            log.warning("Нет подключения к CRM, профили пока не сохраняем: %s", exc)
            self._pool = None
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def exists(self, user_id: str) -> bool:
        pool = await self.connect()
        if pool is None:
            return True  # без БД не блокируем вход экраном согласия
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM user_profiles WHERE user_id = $1", user_id
            )
        return row is not None

    async def upsert(self, user_id: str, claims: dict, provider: str) -> None:
        pool = await self.connect()
        if pool is None:
            log.warning("CRM недоступна, профиль %s не сохранён", user_id)
            return
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_profiles (
                    user_id, username, email, full_name, identity_provider,
                    raw_claims, consent_granted, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, true, now(), now())
                ON CONFLICT (user_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    email = EXCLUDED.email,
                    full_name = EXCLUDED.full_name,
                    identity_provider = EXCLUDED.identity_provider,
                    raw_claims = EXCLUDED.raw_claims,
                    consent_granted = true,
                    updated_at = now()
                """,
                user_id,
                claims.get("preferred_username"),
                claims.get("email"),
                claims.get("name") or claims.get("preferred_username"),
                provider,
                json.dumps(claims, ensure_ascii=False),
            )
