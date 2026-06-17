import logging
import re

import pandas as pd

from core.utils import (
    INT_METABASE_DATASET,
    PROJECT_NAME,
    RAW_METABASE_DATASET,
)

logger = logging.getLogger(__name__)

ARCHIVE_NAME_PREFIX = "[Archive] - "


def _like_to_regex(pattern):
    """Translate a SQL-LIKE pattern (`%`, `_`) to a case-insensitive regex."""
    out = []
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    return re.compile("".join(out), re.IGNORECASE)


def _summarize_metabase_error(result):
    """One-line summary of a Metabase API error JSON. Pulls the human-readable
    cause and any actionable hints (e.g. non-remote-synced-models) out of the
    Clojure stacktrace blob so it doesn't drown the log."""
    if not isinstance(result, dict):
        return repr(result)[:200]
    cause = result.get("cause") or result.get("message")
    if not cause:
        via = result.get("via")
        if isinstance(via, list) and via and isinstance(via[0], dict):
            cause = via[0].get("message")
    if not cause:
        # Non-JSON responses (e.g. an IAP login-page redirect) land here with
        # the raw body captured by update_card_collections — surface it.
        cause = result.get("body")
    data = result.get("data") or {}
    extras = []
    for key in ("status-code", "non-remote-synced-models"):
        if data.get(key) is not None:
            extras.append(f"{key}={data[key]}")
    extras_str = " ".join(extras)
    if extras_str:
        return f"{cause or 'unknown error'} [{extras_str}]"
    return cause or "unknown error"


def _ancestors_match(regex, ancestors):
    if ancestors is None:
        return False
    try:
        return any(regex.search(name) for name in ancestors if name)
    except TypeError:
        return False


def _first_ancestor(names):
    """Top-of-chain ancestor name for display. Robust to None, empty, list,
    tuple, or numpy.ndarray (which is what pd.read_gbq returns for ARRAY columns).
    """
    if names is None:
        return "(no ancestor)"
    try:
        if len(names) == 0:
            return "(no ancestor)"
        first = names[0]
        return first if first else "(no ancestor)"
    except TypeError:
        return "(no ancestor)"


def load_activity_dataframe(dataset=None, table="activity"):
    """Load the full activity table as a DataFrame.

    Defaults to `{INT_METABASE_DATASET}.activity`. Overrides allow pointing
    at a sandbox table for dry-run / pre-deploy testing.
    """
    dataset = dataset or INT_METABASE_DATASET
    query = f"SELECT * FROM `{dataset}.{table}`"
    return pd.read_gbq(query)


def _scope_breakdown_query():
    """Total card count split by collection scope (global vs personal)."""
    return f"""
        SELECT
            CASE
                WHEN c.collection_id IS NULL THEN 'global_root'
                WHEN c.personal_owner_id IS NULL THEN 'global'
                ELSE 'personal'
            END AS scope,
            COUNT(*) AS cards
        FROM `{RAW_METABASE_DATASET}.metabase_report_card` AS rc
        LEFT JOIN `{RAW_METABASE_DATASET}.metabase_collection` AS c
            ON rc.card_collection_id = c.collection_id
        GROUP BY 1
        ORDER BY scope
    """


def _hard_archive_breakdown_query(name_pattern, min_days_since_update):
    """Hard-archive candidates grouped by top-level (root-child) collection.

    Excludes cards in personal collections AND cards living in any descendant
    of a personal collection root — they are out of scope for archiving and
    would never be acted on (see the action query in `hard_archive_stale_cards`).
    """
    return f"""
        WITH coll_root AS (
            SELECT
                c.collection_id,
                c.personal_owner_id AS own_personal_owner_id,
                SAFE_CAST(
                    SPLIT(
                        TRIM(CONCAT(c.location, c.collection_id, '/'), '/'),
                        '/'
                    )[SAFE_OFFSET(0)] AS INT64
                ) AS root_id
            FROM `{RAW_METABASE_DATASET}.metabase_collection` AS c
        ),
        already_hard_archived AS (
            SELECT DISTINCT id
            FROM `{INT_METABASE_DATASET}.archiving_log`
            WHERE object_type = 'card_hard_archive'
              AND status IN ('success', 'already_archived')
        ),
        hard_candidates AS (
            SELECT rc.id, coll_root.root_id
            FROM `{RAW_METABASE_DATASET}.metabase_report_card` AS rc
            LEFT JOIN coll_root ON rc.card_collection_id = coll_root.collection_id
            LEFT JOIN `{RAW_METABASE_DATASET}.metabase_collection` AS root_coll
                ON coll_root.root_id = root_coll.collection_id
            WHERE REGEXP_CONTAINS(rc.card_name, r'(?i){name_pattern}')
              AND date(rc.updated_at) < date_sub(
                  current_date(), interval {int(min_days_since_update)} day
              )
              AND coll_root.own_personal_owner_id IS NULL
              AND root_coll.personal_owner_id IS NULL
              AND rc.id NOT IN (SELECT id FROM already_hard_archived)
        )
        SELECT
            COALESCE(LOWER(top_c.collection_name), '(no collection)') AS top_folder,
            COUNT(*) AS cards
        FROM hard_candidates AS h
        LEFT JOIN `{RAW_METABASE_DATASET}.metabase_collection` AS top_c
            ON h.root_id = top_c.collection_id
        GROUP BY 1
        ORDER BY cards DESC
    """


def compute_archive_stats(activity_df, config, excluded_ids=None):
    """Pre-flight stats for the archive command. Pure read — no mutations.

    Returns a dict with:
      - scope_breakdown: DataFrame of total cards split by scope (global / personal)
      - soft_total / soft_by_folder: soft-archive candidate counts
      - soft_excluded: count of cards skipped via the failure cooldown
      - hard_total / hard_by_folder: hard-archive candidate counts (BQ-driven)
      - cleanup_root_collection_ids / cleanup_min_age_days: echo of cleanup config

    Empty-collection / dead-dashboard counts are NOT included because they
    require an API walk; use `archive --dry-run` to see those per-item.
    """
    stats = {"soft_excluded": len(excluded_ids or set())}

    stats["scope_breakdown"] = pd.read_gbq(_scope_breakdown_query())

    soft_rules = config.get("soft_archive_rules", [])
    if soft_rules and activity_df is not None and not activity_df.empty:
        candidates = select_soft_archive_candidates(
            activity_df, soft_rules, excluded_ids=excluded_ids
        )
        if candidates:
            cand_df = pd.DataFrame(candidates)
            ancestor_map = activity_df.set_index("card_id")[
                "ancestor_collection_names"
            ].apply(_first_ancestor)
            cand_df["top_folder"] = cand_df["id"].map(ancestor_map).fillna("(unknown)")
            stats["soft_total"] = len(cand_df)
            stats["soft_by_folder"] = (
                cand_df.groupby(["rule_name", "top_folder"])
                .size()
                .reset_index(name="cards")
                .sort_values(["rule_name", "cards"], ascending=[True, False])
            )
        else:
            stats["soft_total"] = 0
            stats["soft_by_folder"] = pd.DataFrame(
                columns=["rule_name", "top_folder", "cards"]
            )
    else:
        stats["soft_total"] = 0
        stats["soft_by_folder"] = pd.DataFrame(
            columns=["rule_name", "top_folder", "cards"]
        )

    hard_cfg = config.get("hard_archive_cards")
    if hard_cfg:
        hard_df = pd.read_gbq(
            _hard_archive_breakdown_query(
                hard_cfg["name_pattern"], hard_cfg["min_days_since_update"]
            )
        )
        stats["hard_by_folder"] = hard_df
        stats["hard_total"] = int(hard_df["cards"].sum()) if not hard_df.empty else 0
    else:
        stats["hard_total"] = 0
        stats["hard_by_folder"] = pd.DataFrame(columns=["top_folder", "cards"])

    cleanup = config.get("empty_cleanup", {}) or {}
    stats["cleanup_root_collection_ids"] = cleanup.get("scan_root_collection_ids", [])
    stats["cleanup_min_age_days"] = cleanup.get("min_age_days")

    return stats


def log_archive_stats(stats):
    """Pretty-print the stats dict."""
    logger.info("=== Archive pre-flight stats ===")
    logger.info(
        "Cards in scope (global vs personal):\n%s",
        stats["scope_breakdown"].to_string(index=False),
    )
    excluded = stats.get("soft_excluded", 0)
    logger.info(
        "Soft-archive candidates: %d (skipped %d previously-failed)",
        stats["soft_total"],
        excluded,
    )
    if stats["soft_total"]:
        logger.info(
            "  by rule + top folder:\n%s",
            stats["soft_by_folder"].to_string(index=False),
        )
    logger.info(
        "Hard-archive candidates: %d (personal collections excluded)",
        stats["hard_total"],
    )
    if stats["hard_total"]:
        logger.info(
            "  by top folder:\n%s",
            stats["hard_by_folder"].to_string(index=False),
        )
    if stats["cleanup_root_collection_ids"]:
        logger.info(
            "Cleanup scan: %d root(s), min_age=%s days, roots=%s "
            "(per-item counts visible in --dry-run)",
            len(stats["cleanup_root_collection_ids"]),
            stats["cleanup_min_age_days"],
            stats["cleanup_root_collection_ids"],
        )
    logger.info("================================")


def load_recently_failed_card_ids(cooldown_days=30):
    """Card IDs that failed to soft-archive in the last `cooldown_days`.

    Used to skip cards that previously hit Metabase API errors (e.g. the
    remote-sync rejection class) so daily runs don't keep retrying the same
    dead-ends. Pass `cooldown_days=None` to disable.
    """
    if cooldown_days is None:
        return set()
    query = f"""
        SELECT DISTINCT id
        FROM `{INT_METABASE_DATASET}.archiving_log`
        WHERE object_type = 'card'
          AND COALESCE(status, '') != 'success'
          AND date(archived_at) >= date_sub(
              current_date(), interval {int(cooldown_days)} day
          )
    """
    df = pd.read_gbq(query)
    if df.empty:
        return set()
    return set(df["id"].astype(int).tolist())


def append_archiving_logs(log_entries):
    """Append a batch of log entries to the archiving_log BQ table in one call."""
    if not log_entries:
        logger.info("No archiving log entries to write")
        return
    table_id = f"{PROJECT_NAME}.{INT_METABASE_DATASET}.archiving_log"
    pd.DataFrame(log_entries).to_gbq(
        table_id,
        project_id=PROJECT_NAME,
        if_exists="append",
    )
    logger.info("Wrote %d archiving log entries to BQ", len(log_entries))


def select_soft_archive_candidates(df, rules, excluded_ids=None):
    """Pick cards to soft-archive. First matching rule wins (rules order = YAML order).

    A card is a candidate for a rule when:
      * its id is NOT in `excluded_ids` (e.g. recent failure cooldown),
      * any of its ancestor collection names matches the rule's title pattern,
      * its card_creation_date is older than rule.min_age_days,
      * its views over rule.views_window_months are strictly below rule.max_views,
      * (optional) its card_update_date is older than rule.min_days_since_update
        — protects cards being actively edited even if they have low traffic.
    """
    excluded_ids = set(excluded_ids) if excluded_ids else set()
    if df is None or df.empty:
        return []

    if "ancestor_collection_names" not in df.columns:
        raise ValueError(
            "Activity table is missing column 'ancestor_collection_names'. "
            "The dbt model `int_metabase__activity` must be redeployed "
            "with the rewrite that exposes the ancestor chain."
        )

    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    creation = pd.to_datetime(df["card_creation_date"]).dt.tz_localize(None)
    age_days = (today - creation).dt.days
    update_age_days = None
    if "card_update_date" in df.columns:
        last_update = pd.to_datetime(df["card_update_date"]).dt.tz_localize(None)
        update_age_days = (today - last_update).dt.days

    candidates = []
    matched_ids = set()

    for rule in rules:
        regex = _like_to_regex(rule["ancestor_title_pattern"])
        views_col = f"total_views_{rule['views_window_months']}_months"
        if views_col not in df.columns:
            raise ValueError(
                f"Activity table is missing required column {views_col!r} "
                f"for rule {rule['name']!r}. Has dbt been redeployed?"
            )

        ancestor_match = df["ancestor_collection_names"].apply(
            lambda names, r=regex: _ancestors_match(r, names)
        )
        rule_mask = (
            ancestor_match
            & (age_days >= rule["min_age_days"])
            & (df[views_col] < rule["max_views"])
            & (~df["card_id"].isin(excluded_ids))
        )
        min_update_age = rule.get("min_days_since_update")
        if min_update_age is not None:
            if update_age_days is None:
                raise ValueError(
                    f"Rule {rule['name']!r} requires 'min_days_since_update' but "
                    "activity table is missing column 'card_update_date'."
                )
            rule_mask = rule_mask & (update_age_days >= min_update_age)

        for _, row in df.loc[rule_mask].iterrows():
            card_id = int(row["card_id"])
            if card_id in matched_ids:
                continue
            matched_ids.add(card_id)
            candidates.append(
                {
                    "id": card_id,
                    "name": row["card_name"],
                    "collection_id": int(row["card_collection_id"]),
                    "rule_name": rule["name"],
                    "last_execution_date": row.get("last_execution_date"),
                    "last_execution_context": row.get("last_execution_context"),
                }
            )

    logger.info("Selected %d soft-archive candidate(s)", len(candidates))
    return candidates


class MoveToArchive:
    def __init__(self, card, archive_collection_id, metabase, dry_run=False):
        self.id = card["id"]
        self.name = card["name"]
        self.collection_id = card["collection_id"]
        self.archive_collection_id = archive_collection_id
        self.rule_name = card.get("rule_name")
        self.last_execution_date = card.get("last_execution_date")
        self.last_execution_context = card.get("last_execution_context")
        self.metabase = metabase
        self.dry_run = dry_run

    def _archive_name(self):
        if re.search("archive", self.name, re.IGNORECASE):
            return self.name
        return ARCHIVE_NAME_PREFIX + self.name

    def rename_archive_object(self):
        archive_name = self._archive_name()
        if self.dry_run:
            logger.info(
                "[DRY-RUN] Would rename card %s: %r → %r",
                self.id,
                self.name,
                archive_name,
            )
            return None
        return self.metabase.put_card(self.id, {"name": archive_name})

    def move_object(self):
        if self.dry_run:
            logger.info(
                "[DRY-RUN] Would move card %s (%r) from collection %s to %s (rule=%s)",
                self.id,
                self.name,
                self.collection_id,
                self.archive_collection_id,
                self.rule_name,
            )
            return None

        result = self.metabase.update_card_collections(
            [self.id], self.archive_collection_id
        )
        raw_status = result.get("status") if isinstance(result, dict) else None
        status = "success" if raw_status == "ok" else raw_status

        log_entry = {
            "id": self.id,
            "object_type": "card",
            "status": status,
            "http_status": (
                result.get("http_status") if isinstance(result, dict) else None
            ),
            "error_reason": (
                None if status == "success" else _summarize_metabase_error(result)
            ),
            "new_collection_id": self.archive_collection_id,
            "previous_collection_id": self.collection_id,
            "archived_at": pd.Timestamp.now(),
            "last_execution_date": self.last_execution_date,
            "last_execution_context": self.last_execution_context,
            "parent_folder": self.rule_name,
        }

        if status == "success":
            logger.info(
                "Moved card %s to collection %s (rule=%s)",
                self.id,
                self.archive_collection_id,
                self.rule_name,
            )
        else:
            logger.error(
                "Failed to move card %s: status=%s reason=%s",
                self.id,
                status,
                _summarize_metabase_error(result),
            )
            logger.debug("Full move response for card %s: %r", self.id, result)

        return log_entry


def hard_archive_stale_cards(
    metabase,
    name_pattern,
    min_days_since_update,
    max_cards,
    failure_cooldown_days=30,
    dry_run=False,
):
    """Hard-archive cards whose name matches `name_pattern` (regex) and that have
    not been updated for at least `min_days_since_update` days.

    Candidate selection is done in BigQuery on `raw.metabase_report_card`
    (no API pagination). The per-card `archived` flag is verified via the API
    before flipping it, since the raw mirror does not expose `archived`.

    Cards that failed to hard-archive in the last `failure_cooldown_days` (e.g.
    Metabase remote-sync rejections) are skipped so a fixed-size batch isn't
    permanently consumed retrying the same dead-ends. Pass None to disable.
    """
    if failure_cooldown_days is None:
        cooldown_cte = ""
        cooldown_filter = ""
    else:
        cooldown_cte = f""",
        recently_failed AS (
            SELECT DISTINCT id
            FROM `{INT_METABASE_DATASET}.archiving_log`
            WHERE object_type = 'card_hard_archive'
              AND COALESCE(status, '') NOT IN ('success', 'already_archived')
              AND date(archived_at) >= date_sub(
                  current_date(), interval {int(failure_cooldown_days)} day
              )
        )"""
        cooldown_filter = "AND rc.id NOT IN (SELECT id FROM recently_failed)"
    query = f"""
        WITH coll_root AS (
            SELECT
                c.collection_id,
                c.personal_owner_id AS own_personal_owner_id,
                SAFE_CAST(
                    SPLIT(
                        TRIM(CONCAT(c.location, c.collection_id, '/'), '/'),
                        '/'
                    )[SAFE_OFFSET(0)] AS INT64
                ) AS root_id
            FROM `{RAW_METABASE_DATASET}.metabase_collection` AS c
        ),
        already_hard_archived AS (
            SELECT DISTINCT id
            FROM `{INT_METABASE_DATASET}.archiving_log`
            WHERE object_type = 'card_hard_archive'
              AND status IN ('success', 'already_archived')
        ){cooldown_cte}
        SELECT rc.id, rc.card_collection_id
        FROM `{RAW_METABASE_DATASET}.metabase_report_card` AS rc
        LEFT JOIN coll_root ON rc.card_collection_id = coll_root.collection_id
        LEFT JOIN `{RAW_METABASE_DATASET}.metabase_collection` AS root_coll
            ON coll_root.root_id = root_coll.collection_id
        WHERE REGEXP_CONTAINS(rc.card_name, r'(?i){name_pattern}')
          AND date(rc.updated_at) < date_sub(current_date(), interval {int(min_days_since_update)} day)
          AND coll_root.own_personal_owner_id IS NULL
          AND root_coll.personal_owner_id IS NULL
          AND rc.id NOT IN (SELECT id FROM already_hard_archived)
          {cooldown_filter}
        ORDER BY rc.id
        LIMIT {int(max_cards)}
    """
    df = pd.read_gbq(query)
    logger.info("%d card(s) candidate for hard-archive", len(df))

    archived_ids = []
    skipped_already_archived = 0
    failed = 0
    log_entries = []
    for _, row in df.iterrows():
        card_id = int(row["id"])

        if dry_run:
            logger.info(
                "[DRY-RUN] Would hard-archive card %s (collection=%s)",
                card_id,
                row["card_collection_id"],
            )
            archived_ids.append(card_id)
            continue

        card = metabase.get_cards(card_id)
        if card.get("archived", False):
            # Already archived in Metabase (manual archive, or a run that
            # predates logging). Record it so the dedup CTE excludes it next
            # run — otherwise these cards permanently fill the LIMIT batch and
            # genuinely-stale candidates are never reached.
            logger.info("Card %s already archived in Metabase, recording skip", card_id)
            skipped_already_archived += 1
            log_entries.append(
                _hard_archive_log_entry(card_id, "already_archived", card)
            )
            continue

        result = metabase.put_card(card_id, {"archived": True})
        # Success returns the updated card with `archived: true`; a remote-sync
        # rejection (or any HTTP error) returns an error body instead. Only
        # confirmed archives count, so failures aren't logged as success and
        # silently dropped by the dedup CTE.
        if isinstance(result, dict) and result.get("archived") is True:
            archived_ids.append(card_id)
            logger.info("Hard-archived card %s", card_id)
            log_entries.append(_hard_archive_log_entry(card_id, "success", card))
        else:
            reason = _summarize_metabase_error(result)
            http_status = (
                result.get("http_status") if isinstance(result, dict) else None
            )
            logger.error("Failed to hard-archive card %s: %s", card_id, reason)
            failed += 1
            log_entries.append(
                _hard_archive_log_entry(
                    card_id,
                    "failed",
                    card,
                    http_status=http_status,
                    error_reason=reason,
                )
            )

    if not dry_run:
        append_archiving_logs(log_entries)

    if archived_ids:
        logger.info("Hard-archived %d card(s): %s", len(archived_ids), archived_ids)
    else:
        logger.info("No stale cards to hard-archive")
    if skipped_already_archived:
        logger.info(
            "Recorded %d card(s) already archived in Metabase (excluded next run)",
            skipped_already_archived,
        )
    if failed:
        logger.warning(
            "%d card(s) failed to hard-archive (skipped for the cooldown window)",
            failed,
        )

    return archived_ids


def _hard_archive_log_entry(card_id, status, card, http_status=None, error_reason=None):
    """Build one archiving_log row for the hard-archive path. All hard-archive
    entries share a stable schema (incl. http_status / error_reason) so a mixed
    success/failure batch appends cleanly."""
    collection_id = card.get("collection_id") if isinstance(card, dict) else None
    return {
        "id": card_id,
        "object_type": "card_hard_archive",
        "status": status,
        "http_status": http_status,
        "error_reason": error_reason,
        "new_collection_id": collection_id,
        "previous_collection_id": collection_id,
        "archived_at": pd.Timestamp.now(),
        "last_execution_date": None,
        "last_execution_context": None,
        "parent_folder": "archive",
    }


def _is_old_enough(created_at, min_age_days):
    if not created_at:
        return False
    try:
        created = pd.to_datetime(created_at, utc=True)
    except Exception:
        return False
    return (pd.Timestamp.utcnow() - created).days >= min_age_days


def _resolve_created_at(item, metabase, fetcher):
    """Return item.created_at, falling back to a single-object API fetch when
    the listing endpoint omits it (varies by Metabase version)."""
    return _resolve_metadata(item, metabase, fetcher, ["created_at"])["created_at"]


def _resolve_metadata(item, metabase, fetcher, fields):
    """Return a dict of {field: value} from `item`, fetching the full object
    once if any requested field is missing in the listing payload."""
    result = {f: item.get(f) for f in fields}
    if any(result[f] is None for f in fields):
        fetched = fetcher(metabase, item["id"]) or {}
        for f in fields:
            if result[f] is None:
                result[f] = fetched.get(f)
    return result


def _passes_age_filters(meta, min_age_days, min_days_since_update):
    """Item is eligible only if it's old enough since creation AND, when
    `min_days_since_update` is set, also old enough since last update."""
    if not _is_old_enough(meta.get("created_at"), min_age_days):
        return False
    if min_days_since_update is not None and not _is_old_enough(
        meta.get("updated_at"), min_days_since_update
    ):
        return False
    return True


def _required_meta_fields(min_days_since_update):
    """Only request `updated_at` when the caller actually filters on it —
    avoids an extra API fetch when the fallback path has to fill it in."""
    fields = ["created_at"]
    if min_days_since_update is not None:
        fields.append("updated_at")
    return fields


def _record_cleanup_candidate(
    log_entries, object_id, object_type, status, parent_collection_id
):
    """Append one archiving_log audit row for a cleanup candidate (an empty
    collection or a dead dashboard) to `log_entries`.

    No-op when `log_entries` is None (caller opted out of auditing). `status`
    is one of: `success` (archived), `skipped_recent` (a genuine candidate kept
    because it's newer than the age threshold), or `dry_run`. Only candidates
    are recorded — live dashboards / non-empty collections are not, so the table
    shows exactly what the cleanup considered and why it acted or didn't."""
    if log_entries is None:
        return
    log_entries.append(
        {
            "id": object_id,
            "object_type": object_type,
            "status": status,
            "new_collection_id": None,
            "previous_collection_id": parent_collection_id,
            "archived_at": pd.Timestamp.now(),
            "last_execution_date": None,
            "last_execution_context": None,
            "parent_folder": f"{object_type}_cleanup",
        }
    )


def archive_dead_dashboards(
    metabase,
    root_collection_ids,
    min_age_days,
    min_days_since_update=None,
    dry_run=False,
):
    """Archive dashboards that are empty or hold only archived cards, that
    were created at least `min_age_days` days ago and (optionally) haven't
    been updated for `min_days_since_update` days. Scans recursively under
    each of `root_collection_ids` (archive root included by the caller)."""
    archived = []
    log_entries = []

    for root_id in root_collection_ids:
        archived.extend(
            _scan_dashboards_recursive(
                metabase,
                root_id,
                min_age_days,
                min_days_since_update,
                dry_run,
                log_entries,
            )
        )

    if not dry_run:
        append_archiving_logs(log_entries)

    prefix = "[DRY-RUN] Would archive" if dry_run else "Archived"
    if archived:
        logger.info("%s %d dead dashboard(s): %s", prefix, len(archived), archived)
    else:
        logger.info("No dead dashboards found")

    return archived


def _scan_dashboards_recursive(
    metabase,
    collection_id,
    min_age_days,
    min_days_since_update=None,
    dry_run=False,
    log_entries=None,
):
    archived = []

    response = metabase.get_collection_children(collection_id)
    items = response.get("data", [])

    for item in items:
        model = item.get("model")
        if model == "collection":
            archived.extend(
                _scan_dashboards_recursive(
                    metabase,
                    item["id"],
                    min_age_days,
                    min_days_since_update,
                    dry_run,
                    log_entries,
                )
            )
        elif model == "dashboard":
            # Deadness first so we can audit dead-but-too-recent dashboards too
            # (the candidates a reader wants to see, not just the archived ones).
            if not _is_dashboard_dead(metabase, item["id"]):
                continue
            meta = _resolve_metadata(
                item,
                metabase,
                lambda m, _id: m.get_dashboards(_id),
                _required_meta_fields(min_days_since_update),
            )
            if not _passes_age_filters(meta, min_age_days, min_days_since_update):
                logger.info(
                    "Dead dashboard %s kept (newer than age threshold)", item["id"]
                )
                _record_cleanup_candidate(
                    log_entries,
                    item["id"],
                    "dashboard",
                    "skipped_recent",
                    collection_id,
                )
                continue
            if dry_run:
                logger.info("[DRY-RUN] Would archive dead dashboard %s", item["id"])
                _record_cleanup_candidate(
                    log_entries, item["id"], "dashboard", "dry_run", collection_id
                )
            else:
                logger.info("Archiving dead dashboard %s", item["id"])
                metabase.put_dashboard(item["id"], {"archived": True})
                _record_cleanup_candidate(
                    log_entries, item["id"], "dashboard", "success", collection_id
                )
            archived.append(item["id"])

    return archived


def _is_dashboard_dead(metabase, dashboard_id):
    """A dashboard is dead when it has no live content.

    `dashcards` is the modern Metabase key; `ordered_cards` is the legacy alias.
    A dashcard is "live" if either:
      * it carries a question card that is not archived, or
      * it is a virtual card (text, heading, link, iframe) — those have no
        `card` field and represent presentation content the user explicitly added.
    """
    dashboard = metabase.get_dashboards(dashboard_id)
    items = dashboard.get("dashcards") or dashboard.get("ordered_cards") or []

    if not items:
        return True

    for item in items:
        card = item.get("card")
        if card is None:
            return False  # virtual card (text/heading/link/iframe)
        if not card.get("archived", False):
            return False

    return True


def _is_collection_empty(metabase, collection_id):
    response = metabase.get_collection_children(collection_id)
    items = response.get("data", [])
    return len(items) == 0


def archive_empty_collections(
    metabase,
    root_collection_ids,
    min_age_days,
    min_days_since_update=None,
    exclude_ids=None,
    dry_run=False,
):
    """Recursively archive empty collections under `root_collection_ids` whose
    `created_at` is older than `min_age_days` and (optionally) `updated_at`
    is older than `min_days_since_update`. Roots themselves are never
    archived (they are added to `exclude_ids` by default)."""
    if exclude_ids is None:
        exclude_ids = set(root_collection_ids)

    archived = []
    log_entries = []

    for root_id in root_collection_ids:
        archived.extend(
            _archive_empty_recursive(
                metabase,
                root_id,
                exclude_ids,
                min_age_days,
                min_days_since_update,
                dry_run,
                log_entries,
            )
        )

    if not dry_run:
        append_archiving_logs(log_entries)

    prefix = "[DRY-RUN] Would archive" if dry_run else "Archived"
    if archived:
        logger.info("%s %d empty collection(s): %s", prefix, len(archived), archived)
    else:
        logger.info("No empty collections found")

    return archived


def _archive_empty_recursive(
    metabase,
    collection_id,
    exclude_ids,
    min_age_days,
    min_days_since_update=None,
    dry_run=False,
    log_entries=None,
):
    archived = []

    response = metabase.get_collection_children(collection_id, models=["collection"])
    children = response.get("data", [])

    for child in children:
        if child.get("model") != "collection":
            continue
        child_id = child["id"]
        sub_archived = _archive_empty_recursive(
            metabase,
            child_id,
            exclude_ids,
            min_age_days,
            min_days_since_update,
            dry_run,
            log_entries,
        )
        archived.extend(sub_archived)

        if child_id in exclude_ids:
            continue
        # Emptiness first so we can audit empty-but-too-recent folders too — the
        # candidates a reader wants to see, not just the archived ones.
        if not _is_collection_empty(metabase, child_id):
            continue
        meta = _resolve_metadata(
            child,
            metabase,
            lambda m, _id: m.get_collections(_id),
            _required_meta_fields(min_days_since_update),
        )
        if not _passes_age_filters(meta, min_age_days, min_days_since_update):
            logger.info("Empty collection %s kept (newer than age threshold)", child_id)
            _record_cleanup_candidate(
                log_entries, child_id, "collection", "skipped_recent", collection_id
            )
            continue

        if dry_run:
            logger.info("[DRY-RUN] Would archive empty collection %s", child_id)
            _record_cleanup_candidate(
                log_entries, child_id, "collection", "dry_run", collection_id
            )
        else:
            logger.info("Archiving empty collection %s", child_id)
            metabase.put_collection(child_id, {"archived": True})
            _record_cleanup_candidate(
                log_entries, child_id, "collection", "success", collection_id
            )
        archived.append(child_id)

    return archived
