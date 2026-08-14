-- Профили пользователей, полученные от внешних удостоверяющих служб
-- (Keycloak, LDAP представительства, Яндекс ID). Заполняет bionicpro-auth
-- после того, как пользователь дал согласие на обработку данных.
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id           TEXT PRIMARY KEY,
    username          TEXT,
    email             TEXT,
    full_name         TEXT,
    identity_provider TEXT NOT NULL DEFAULT 'keycloak',
    raw_claims        JSONB,
    consent_granted   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_username ON user_profiles (username);
