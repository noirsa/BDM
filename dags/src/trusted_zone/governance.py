from __future__ import annotations

import os
from typing import Any


GOVERNANCE_METADATA_FIELDS = [
    "source_system",
    "ingestion_time",
    "source_file_path",
    "validation_status",
    "schema_version",
]

TRUSTED_SCHEMA_VERSION = "trusted_v1"
ALLOWED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


def governance_metadata(
    *,
    source_system: str,
    ingestion_time: str,
    source_file_path: str,
    validation_status: str = "valid",
    schema_version: str = TRUSTED_SCHEMA_VERSION,
) -> dict[str, str]:
    """Build the minimal Trusted Zone governance metadata shared by all outputs."""
    return {
        "source_system": source_system,
        "ingestion_time": ingestion_time,
        "source_file_path": source_file_path,
        "validation_status": validation_status,
        "schema_version": schema_version,
    }


def is_allowed_image_key(object_key: str) -> bool:
    """Return True when an image key has an allowed extension."""
    return os.path.splitext(object_key.lower())[1] in ALLOWED_IMAGE_EXTENSIONS


def has_required_keys(record: dict[str, Any], required_fields: list[str] | tuple[str, ...] | None) -> bool:
    """Check record-level required fields when a source already defines them."""
    if not required_fields:
        return True
    return all(field in record and record[field] is not None for field in required_fields)
