import os


class Settings:
    keycloak_url = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
    realm = os.getenv("KEYCLOAK_REALM", "reports-realm")
    # Keycloak в issuer подставляет тот адрес, по которому к нему пришли из браузера
    expected_issuers = [
        i.strip()
        for i in os.getenv(
            "EXPECTED_ISSUERS",
            "http://localhost:8080/realms/reports-realm,http://keycloak:8080/realms/reports-realm",
        ).split(",")
        if i.strip()
    ]
    required_role = os.getenv("REQUIRED_ROLE", "prothetic_user")
    verify_audience = os.getenv("VERIFY_AUDIENCE", "").strip()

    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_db = os.getenv("CLICKHOUSE_DB", "bionicpro")
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "analytics")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "analytics_password")
    # переключение между витриной Airflow и витриной, собранной через CDC
    mart_table = os.getenv("REPORT_MART_TABLE", "user_report_mart")

    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    s3_bucket = os.getenv("S3_BUCKET", "reports")
    s3_region = os.getenv("S3_REGION", "us-east-1")
    cdn_base_url = os.getenv("CDN_BASE_URL", "http://localhost:8081")
    # срок жизни подписанной ссылки на отчёт
    link_ttl_seconds = int(os.getenv("REPORT_LINK_TTL_SECONDS", "900"))

    default_period_days = int(os.getenv("DEFAULT_PERIOD_DAYS", "30"))

    @property
    def jwks_url(self) -> str:
        return f"{self.keycloak_url}/realms/{self.realm}/protocol/openid-connect/certs"


settings = Settings()
