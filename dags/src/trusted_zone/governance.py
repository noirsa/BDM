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

CATALOGUE_POLICY_FIELDS = [
    "owner",
    "data_steward",
    "data_classification",
    "pii_flag",
    "retention_policy",
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


def catalogue_policy_metadata(record: dict[str, Any]) -> dict[str, str]:
    """Return lightweight policy metadata for the trusted catalogue summary."""
    dataset_name = str(record.get("dataset_name", "")).lower()
    source_type = str(record.get("source_type", "")).lower()
    validation_status = str(record.get("validation_status", "valid")).lower()

    if validation_status == "rejected":
        classification = "quality_review"
        pii_flag = "review_required"
    elif "tweet" in dataset_name:
        classification = "public_text_analytics"
        pii_flag = "possible_user_mentions"
    elif source_type == "semi_structured":
        classification = "public_environmental_observation"
        pii_flag = "no_direct_pii"
    elif source_type == "unstructured":
        classification = "public_image_metadata"
        pii_flag = "no_direct_pii"
    else:
        classification = "public_environmental_analytics"
        pii_flag = "no_direct_pii"

    return {
        "owner": "data_engineering_team",
        "data_steward": "bdm_project_team",
        "data_classification": classification,
        "pii_flag": pii_flag,
        "retention_policy": "course_project_retained_until_assessment_archive",
    }
