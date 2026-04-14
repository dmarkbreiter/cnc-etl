from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from settings import settings


class InvalidJsonBodyError(RuntimeError):
    code = "InvalidJsonBody"


@dataclass
class SpacesObject:
    key: str
    ttl_ms: int = 5 * 60 * 1000

    bucket: str = settings.do_spaces_bucket
    endpoint_url: str = settings.do_spaces_endpoint
    region_name: str = settings.do_spaces_region

    def __post_init__(self) -> None:
        access_key = settings.do_spaces_key.get_secret_value()
        secret_key = settings.do_spaces_secret.get_secret_value()

        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}),
        )

    @property
    def object_key(self) -> str:
        return f"{self.key}.json"

    def get_object_json(self) -> Dict[str, Any]:
        response = self.s3.get_object(Bucket=self.bucket, Key=self.object_key)
        body = response.get("Body")
        if body is None:
            return {}

        raw = body.read()
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        trimmed = text.strip()
        if not trimmed:
            return {}

        # Spaces may return XML for certain errors; avoid crashing JSON consumers.
        if trimmed.startswith("<"):
            raise InvalidJsonBodyError("Expected JSON but received XML")

        return json.loads(trimmed)

    def get_content(self) -> Dict[str, Any]:
        try:
            value = self.get_object_json()
            return value if isinstance(value, dict) else {}
        except ClientError as e:
            code = (e.response.get("Error") or {}).get("Code")
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code == "NoSuchKey" or status == 404:
                return {}
            return {}
        except Exception:
            return {}

    def is_fresh(self, timestamp: int) -> bool:
        return (int(time.time() * 1000) - int(timestamp)) < int(self.ttl_ms)

    def is_not_equal(self, obj: Any) -> bool:
        content = self.get_content()
        return _strip_timestamp(content) != _strip_timestamp(obj)

    def upload(self, data: Any) -> Dict[str, Any]:
        body_text = json.dumps(data)
        body_bytes = body_text.encode("utf-8")
        # Metadata for s3 object upload with timestamp
        metadata = {"uploaded_at": str(int(time.time()))}
        return self.s3.put_object(
            Bucket=self.bucket,
            Key=self.object_key,
            Body=body_bytes,
            ContentLength=len(body_bytes),
            ACL="public-read",
            ContentType="application/json",
            # CacheControl is set to allow caching by browsers and CDNs, but requires revalidation to ensure freshness.
            CacheControl="must-revalidate",
            Metadata=metadata,
        )


def is_fresh_timestamp(
    timestamp: Optional[int] = None, ttl_ms: int = 5 * 60 * 1000
) -> bool:
    if timestamp is None:
        return False
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return False
    return (int(time.time() * 1000) - ts) < int(ttl_ms)


def _strip_timestamp(value: Any) -> Any:
    # Mirrors the TS behavior: only ignore the top-level "timestamp" key.
    if isinstance(value, dict):
        copied = dict(value)
        copied["timestamp"] = 0
        return copied
    return value
