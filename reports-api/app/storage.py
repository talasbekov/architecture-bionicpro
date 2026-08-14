import json
import logging
from typing import Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from .config import settings

log = logging.getLogger(__name__)


class ReportStorage:
    """Готовые отчёты в объектном хранилище с S3 API (MinIO)."""

    def __init__(self) -> None:
        self._s3 = self._build_client(settings.s3_endpoint)
        # Отдельный клиент для подписи ссылок: подпись SigV4 включает host,
        # поэтому ссылку надо подписывать на адрес CDN, а не на внутренний адрес MinIO.
        self._signer = self._build_client(settings.cdn_base_url)

    @staticmethod
    def _build_client(endpoint: str):
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def ensure_bucket(self) -> None:
        try:
            self._s3.head_bucket(Bucket=settings.s3_bucket)
        except ClientError:
            try:
                self._s3.create_bucket(Bucket=settings.s3_bucket)
                log.info("Создан бакет %s", settings.s3_bucket)
            except ClientError as exc:
                log.warning("Не удалось создать бакет: %s", exc)

    @staticmethod
    def build_key(user_id: str, date_from: str, date_to: str, version: str) -> str:
        """Ключ включает пользователя и период, поэтому отчёт находится за одно обращение.
        Версия витрины в ключе даёт естественную инвалидацию: после пересчёта ETL
        меняется путь, старый объект в CDN больше не запрашивается. Имя витрины тоже
        входит в ключ — иначе после переключения на витрину CDC отдавались бы
        объекты, посчитанные по старой."""
        return f"{user_id}/{settings.mart_table}/{version}/{date_from}_{date_to}.json"

    def exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=settings.s3_bucket, Key=key)
            return True
        except ClientError:
            return False

    def get(self, key: str) -> Optional[dict]:
        try:
            obj = self._s3.get_object(Bucket=settings.s3_bucket, Key=key)
            return json.loads(obj["Body"].read())
        except ClientError:
            return None

    def put(self, key: str, report: dict) -> None:
        self._s3.put_object(
            Bucket=settings.s3_bucket,
            Key=key,
            Body=json.dumps(report, ensure_ascii=False, default=str).encode("utf-8"),
            ContentType="application/json; charset=utf-8",
            CacheControl="public, max-age=86400",
        )

    def cdn_url(self, key: str) -> str:
        """Ссылка на CDN с подписью на ограниченное время. Nginx кеширует объект
        по пути без учёта параметров подписи, поэтому кеш переиспользуется,
        а сама ссылка остаётся одноразовой и чужой отчёт по ней не получить."""
        return self._signer.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.s3_bucket, "Key": key},
            ExpiresIn=settings.link_ttl_seconds,
        )
