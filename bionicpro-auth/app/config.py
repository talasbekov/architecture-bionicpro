import os


def _bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes")


class Settings:
    """Настройки сервиса. Всё берём из окружения, дефолты рассчитаны на docker-compose."""

    # Keycloak
    keycloak_url = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
    # адрес, по которому Keycloak доступен из браузера пользователя
    keycloak_public_url = os.getenv("KEYCLOAK_PUBLIC_URL", "http://localhost:8080")
    realm = os.getenv("KEYCLOAK_REALM", "reports-realm")
    client_id = os.getenv("KEYCLOAK_CLIENT_ID", "reports-frontend")
    # публичный клиент секрет не использует, но оставляем возможность confidential
    client_secret = os.getenv("KEYCLOAK_CLIENT_SECRET", "")
    scope = os.getenv("OAUTH_SCOPE", "openid profile email")

    # Куда Keycloak вернёт пользователя после логина
    redirect_uri = os.getenv("AUTH_REDIRECT_URI", "http://localhost:8000/auth/callback")
    frontend_url = os.getenv("FRONTEND_URL", "http://localhost:3000")

    # Сессии
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    session_cookie = os.getenv("SESSION_COOKIE_NAME", "bp_session")
    # Время жизни сессии заведомо больше времени жизни access_token (2 минуты),
    # чтобы протухший access_token можно было обновить по refresh_token.
    session_ttl = int(os.getenv("SESSION_TTL_SECONDS", "1800"))
    login_flow_ttl = int(os.getenv("LOGIN_FLOW_TTL_SECONDS", "300"))
    cookie_secure = _bool("COOKIE_SECURE", "true")
    cookie_samesite = os.getenv("COOKIE_SAMESITE", "lax")
    cookie_domain = os.getenv("COOKIE_DOMAIN") or None
    # Ключ шифрования содержимого сессии (Fernet, base64). Если не задан — генерируется на старте.
    encryption_key = os.getenv("SESSION_ENCRYPTION_KEY", "")
    rotate_session = _bool("ROTATE_SESSION", "true")

    # Куда проксируем защищённые вызовы
    reports_api_url = os.getenv("REPORTS_API_URL", "http://reports-api:8100")

    cors_origins = [
        o.strip()
        for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
        if o.strip()
    ]

    @property
    def issuer(self) -> str:
        return f"{self.keycloak_url}/realms/{self.realm}"

    @property
    def public_issuer(self) -> str:
        return f"{self.keycloak_public_url}/realms/{self.realm}"

    @property
    def authorization_endpoint(self) -> str:
        return f"{self.public_issuer}/protocol/openid-connect/auth"

    @property
    def token_endpoint(self) -> str:
        return f"{self.issuer}/protocol/openid-connect/token"

    @property
    def logout_endpoint(self) -> str:
        return f"{self.issuer}/protocol/openid-connect/logout"

    @property
    def userinfo_endpoint(self) -> str:
        return f"{self.issuer}/protocol/openid-connect/userinfo"


settings = Settings()
