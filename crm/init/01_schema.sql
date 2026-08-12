-- Схема CRM BionicPRO: клиенты и их протезы.
-- Файл монтируется в /docker-entrypoint-initdb.d контейнера crm_db и выполняется при первом старте.

CREATE TABLE IF NOT EXISTS clients (
    id         BIGSERIAL PRIMARY KEY,
    -- user_id совпадает с preferred_username пользователя в Keycloak
    user_id    TEXT UNIQUE NOT NULL,
    full_name  TEXT NOT NULL,
    email      TEXT,
    country    TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS prostheses (
    id            BIGSERIAL PRIMARY KEY,
    client_id     BIGINT NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    serial_number TEXT UNIQUE NOT NULL,
    model         TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',
    activated_at  DATE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_prostheses_client_id ON prostheses (client_id);

-- Тестовые клиенты. prothetic1..3 заведены прямо в Keycloak,
-- john.doe / jane.smith / alex.johnson приезжают из LDAP.
INSERT INTO clients (user_id, full_name, email, country) VALUES
    ('prothetic1',   'Иван Петров',      'prothetic1@example.com', 'Russia'),
    ('prothetic2',   'Мария Соколова',   'prothetic2@example.com', 'Russia'),
    ('prothetic3',   'Дмитрий Кузнецов', 'prothetic3@example.com', 'Russia'),
    ('john.doe',     'John Doe',         'john@example.com',       'Kazakhstan'),
    ('jane.smith',   'Jane Smith',       'jane@example.com',       'Kazakhstan'),
    ('alex.johnson', 'Alex Johnson',     'alex@example.com',       'Kazakhstan')
ON CONFLICT (user_id) DO NOTHING;

-- Протезы: у части клиентов по два устройства (правая и левая рука).
INSERT INTO prostheses (client_id, serial_number, model, status, activated_at)
SELECT c.id, v.serial_number, v.model, v.status, v.activated_at
FROM (VALUES
    ('prothetic1',   'BP-2024-0001', 'BionicArm X1',  'active',      DATE '2024-01-15'),
    ('prothetic1',   'BP-2024-0002', 'BionicArm X1',  'active',      DATE '2024-03-02'),
    ('prothetic2',   'BP-2024-0003', 'BionicArm X2',  'active',      DATE '2024-02-10'),
    ('prothetic3',   'BP-2024-0004', 'BionicLeg L1',  'active',      DATE '2024-04-21'),
    ('prothetic3',   'BP-2024-0005', 'BionicArm X2',  'maintenance', DATE '2024-05-05'),
    ('john.doe',     'BP-2024-0006', 'BionicArm X1',  'active',      DATE '2024-02-28'),
    ('jane.smith',   'BP-2024-0007', 'BionicHand H3', 'active',      DATE '2024-06-11'),
    ('jane.smith',   'BP-2024-0008', 'BionicArm X2',  'active',      DATE '2024-07-01'),
    ('alex.johnson', 'BP-2024-0009', 'BionicLeg L2',  'active',      DATE '2024-03-19'),
    ('alex.johnson', 'BP-2024-0010', 'BionicHand H3', 'inactive',    DATE '2024-08-08')
) AS v(user_id, serial_number, model, status, activated_at)
JOIN clients c ON c.user_id = v.user_id
ON CONFLICT (serial_number) DO NOTHING;

-- Нужно Debezium: в WAL попадает полный образ строки до изменения (в т.ч. для DELETE)
ALTER TABLE clients REPLICA IDENTITY FULL;
ALTER TABLE prostheses REPLICA IDENTITY FULL;
