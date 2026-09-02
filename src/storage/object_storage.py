from __future__ import annotations

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from config.settings import Settings, get_settings

_MISSING_CODES = {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}


class S3ObjectStorage:
    """S3-compatible object storage (MinIO in Docker) for landing and transformed buckets."""

    def __init__(self, settings: Settings | None = None, client=None) -> None:
        self._settings = settings or get_settings()
        self._client = client or boto3.client(
            "s3",
            endpoint_url=self._settings.s3_endpoint,
            aws_access_key_id=self._settings.s3_access_key,
            aws_secret_access_key=self._settings.s3_secret_key,
            region_name=self._settings.s3_region,
            # MinIO needs path-style addressing; virtual-host style resolves to a bad DNS name.
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def ensure_buckets(self) -> None:
        for bucket in (self._settings.s3_landing_bucket, self._settings.s3_transformed_bucket):
            try:
                self._client.head_bucket(Bucket=bucket)
            except ClientError as exc:
                if _error_code(exc) not in _MISSING_CODES:
                    raise
                self._client.create_bucket(Bucket=bucket)

    def put(self, bucket: str, key: str, body: bytes, content_type: str) -> None:
        self._client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

    def get(self, bucket: str, key: str) -> bytes:
        return self._client.get_object(Bucket=bucket, Key=key)["Body"].read()

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if _error_code(exc) in _MISSING_CODES:
                return False
            raise
        return True


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))
