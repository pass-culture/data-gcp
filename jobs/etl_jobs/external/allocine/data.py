import hashlib
import json
import logging
import re
from typing import Any

from google.cloud.bigquery import SchemaField

logger = logging.getLogger(__name__)

# Columns serialized to JSON strings for BigQuery compatibility
COMPLEX_COLUMNS = ["releases", "countries", "genres", "companies", "cast_normalized", "credits_normalized"]

# Ordered list of columns for the staging table (API output + content_hash)
STAGING_COLUMNS = [
    "movie_id",
    "internalId",
    "title",
    "originalTitle",
    "type",
    "runtime",
    "synopsis",
    "poster_url",
    "backlink_url",
    "backlink_label",
    "data_eidr",
    "data_productionYear",
    "cast_normalized",
    "credits_normalized",
    "releases",
    "countries",
    "genres",
    "companies",
    "content_hash",
]

# Schema for staging_movies — mirrors the API output + content_hash
STAGING_SCHEMA: list[SchemaField] = [
    SchemaField("movie_id", "STRING", mode="REQUIRED"),
    SchemaField("internalId", "STRING"),
    SchemaField("title", "STRING"),
    SchemaField("originalTitle", "STRING"),
    SchemaField("type", "STRING"),
    SchemaField("runtime", "INTEGER"),
    SchemaField("synopsis", "STRING"),
    SchemaField("poster_url", "STRING"),
    SchemaField("backlink_url", "STRING"),
    SchemaField("backlink_label", "STRING"),
    SchemaField("data_eidr", "STRING"),
    SchemaField("data_productionYear", "INTEGER"),
    SchemaField("cast_normalized", "STRING"),
    SchemaField("credits_normalized", "STRING"),
    SchemaField("releases", "STRING"),
    SchemaField("countries", "STRING"),
    SchemaField("genres", "STRING"),
    SchemaField("companies", "STRING"),
    SchemaField("content_hash", "STRING"),
]

# Extra columns on raw_movies for poster tracking
_RAW_EXTRA: list[SchemaField] = [
    SchemaField("poster_to_download", "BOOL"),
    SchemaField("poster_gcs_path", "STRING"),
    SchemaField("retry_count", "INTEGER"),
]

RAW_SCHEMA: list[SchemaField] = STAGING_SCHEMA + _RAW_EXTRA


def parse_runtime_to_minutes(raw: str | None) -> int | None:
    if not raw:
        return None
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", str(raw))
    if not match:
        return None
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    total = hours * 60 + minutes + (1 if seconds >= 30 else 0)
    return total if total > 0 else None


def sanitize_text(text: str | None) -> str:
    if text is None:
        return ""
    return text.replace("&#039;", "\u2019")


def normalize_cast(cast_edges: list[dict] | None) -> list[dict]:
    if not cast_edges:
        return []
    result = []
    for edge in cast_edges:
        node = edge.get("node", {}) if isinstance(edge, dict) else {}
        person = node.get("actor") or {}
        result.append(
            {
                "firstName": person.get("firstName") or "",
                "lastName": person.get("lastName") or "",
                "role": node.get("role") or "",
            }
        )
    return result


def normalize_credits(credits_edges: list[dict] | None) -> list[dict]:
    if not credits_edges:
        return []
    result = []
    for edge in credits_edges:
        node = edge.get("node", {}) if isinstance(edge, dict) else {}
        person = node.get("person") or {}
        position = node.get("position") or {}
        result.append(
            {
                "person_firstName": person.get("firstName") or "",
                "person_lastName": person.get("lastName") or "",
                "position_name": position.get("name") or "",
                "role": node.get("role") or "",
                "job": node.get("job") or "",
            }
        )
    return result


def compute_hash(data: dict) -> str:
    serialized = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(serialized.encode("utf-8")).hexdigest()


def transform_movie(raw: dict) -> dict[str, Any]:
    poster = raw.get("poster") or {}
    backlink = raw.get("backlink") or {}
    data_section = raw.get("data") or {}

    row: dict[str, Any] = {
        "movie_id": raw.get("id"),
        "internalId": raw.get("internalId"),
        "title": sanitize_text(raw.get("title")),
        "originalTitle": sanitize_text(raw.get("originalTitle")),
        "type": raw.get("type"),
        "runtime": parse_runtime_to_minutes(raw.get("runtime")),
        "synopsis": sanitize_text(raw.get("synopsis")),
        "poster_url": poster.get("url"),
        "backlink_url": backlink.get("url"),
        "backlink_label": backlink.get("label"),
        "data_eidr": data_section.get("eidr"),
        "data_productionYear": data_section.get("productionYear"),
        "cast_normalized": normalize_cast((raw.get("cast") or {}).get("edges")),
        "credits_normalized": normalize_credits((raw.get("credits") or {}).get("edges")),
        "releases": raw.get("releases") or [],
        "countries": raw.get("countries") or [],
        "genres": raw.get("genres") or [],
        "companies": raw.get("companies") or [],
    }

    # Compute hash before serializing complex columns so the hash is stable
    row["content_hash"] = compute_hash({k: v for k, v in row.items()})

    # Serialize complex columns to JSON strings for BigQuery
    for col in COMPLEX_COLUMNS:
        row[col] = json.dumps(row[col], ensure_ascii=False, default=str)

    return {col: row.get(col) for col in STAGING_COLUMNS}
