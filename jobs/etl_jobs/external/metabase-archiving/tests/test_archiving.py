from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from google.cloud import bigquery

from domain.archiving import (
    ARCHIVING_LOG_SCHEMA,
    MoveToArchive,
    _first_ancestor,
    _is_collection_empty,
    _is_dashboard_dead,
    _is_old_enough,
    _like_to_regex,
    _summarize_metabase_error,
    append_archiving_logs,
    archive_dead_dashboards,
    archive_empty_collections,
    compute_archive_stats,
    hard_archive_stale_cards,
    load_activity_dataframe,
    load_recently_failed_card_ids,
    log_archive_stats,
    select_soft_archive_candidates,
)

THEMATIC_RULE = {
    "name": "thematic",
    "ancestor_title_pattern": "%suivi par thématiques%",
    "min_age_days": 60,
    "views_window_months": 6,
    "max_views": 5,
}

ADHOC_RULE = {
    "name": "adhoc",
    "ancestor_title_pattern": "%adhoc%",
    "min_age_days": 15,
    "views_window_months": 3,
    "max_views": 5,
}


def _activity_row(
    card_id=1,
    name="Card",
    collection_id=10,
    ancestors=None,
    age_days=120,
    update_age_days=None,
    views_3m=0,
    views_6m=0,
):
    if ancestors is None:
        ancestors = ["[adhoc]", "Parent A"]
    if update_age_days is None:
        update_age_days = age_days
    now = pd.Timestamp.utcnow().tz_localize(None)
    return {
        "card_id": card_id,
        "card_name": name,
        "card_collection_id": collection_id,
        "ancestor_collection_names": ancestors,
        "card_creation_date": now - pd.Timedelta(days=age_days),
        "card_update_date": now - pd.Timedelta(days=update_age_days),
        "total_views_3_months": views_3m,
        "total_views_6_months": views_6m,
        "last_execution_date": pd.Timestamp("2024-01-01"),
        "last_execution_context": "api",
    }


class TestLikeToRegex:
    def test_percent_wildcard(self):
        r = _like_to_regex("%adhoc%")
        assert r.search("[Adhoc]") is not None
        assert r.search("Some adHoc text") is not None
        assert r.search("operationnel") is None

    def test_underscore_wildcard(self):
        r = _like_to_regex("a_c")
        assert r.search("abc") is not None
        assert r.search("ac") is None

    def test_special_chars_escaped(self):
        r = _like_to_regex("%suivi par thématiques%")
        assert r.search("Suivi par thématiques 2024") is not None
        assert r.search("operational") is None


class TestLoadActivityDataframe:
    @patch("domain.archiving.pd.read_gbq")
    def test_query_targets_activity_table(self, mock_read_gbq):
        mock_read_gbq.return_value = pd.DataFrame()
        load_activity_dataframe()
        query = mock_read_gbq.call_args[0][0]
        assert "int_metabase_dev.activity" in query

    @patch("domain.archiving.pd.read_gbq")
    def test_dataset_and_table_override(self, mock_read_gbq):
        mock_read_gbq.return_value = pd.DataFrame()
        load_activity_dataframe(dataset="sandbox_xyz", table="activity_test")
        query = mock_read_gbq.call_args[0][0]
        assert "sandbox_xyz.activity_test" in query
        assert "int_metabase_dev" not in query


class TestSelectSoftArchiveCandidates:
    def test_empty_df_returns_empty(self):
        assert select_soft_archive_candidates(pd.DataFrame(), [ADHOC_RULE]) == []

    def test_none_df_returns_empty(self):
        assert select_soft_archive_candidates(None, [ADHOC_RULE]) == []

    def test_adhoc_match_simple(self):
        df = pd.DataFrame([_activity_row(card_id=1, ancestors=["[adhoc]"])])
        result = select_soft_archive_candidates(df, [ADHOC_RULE])
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["rule_name"] == "adhoc"

    def test_thematic_match_with_french_accents(self):
        df = pd.DataFrame(
            [
                _activity_row(
                    card_id=1,
                    ancestors=["Suivi par Thématiques"],
                    age_days=120,
                    views_6m=0,
                )
            ]
        )
        result = select_soft_archive_candidates(df, [THEMATIC_RULE])
        assert len(result) == 1
        assert result[0]["rule_name"] == "thematic"

    def test_deep_hierarchy_matches(self):
        df = pd.DataFrame(
            [_activity_row(ancestors=["[Adhoc]", "Parent A", "Sub Folder"])]
        )
        result = select_soft_archive_candidates(df, [ADHOC_RULE])
        assert len(result) == 1

    def test_no_ancestor_match_excluded(self):
        df = pd.DataFrame([_activity_row(ancestors=["Production", "Team X"])])
        result = select_soft_archive_candidates(df, [ADHOC_RULE, THEMATIC_RULE])
        assert result == []

    def test_age_below_threshold_excluded(self):
        df = pd.DataFrame(
            [_activity_row(ancestors=["[adhoc]"], age_days=5)]  # < 15 days
        )
        result = select_soft_archive_candidates(df, [ADHOC_RULE])
        assert result == []

    def test_views_above_threshold_excluded(self):
        df = pd.DataFrame(
            [_activity_row(ancestors=["[adhoc]"], age_days=20, views_3m=10)]
        )
        result = select_soft_archive_candidates(df, [ADHOC_RULE])
        assert result == []

    def test_views_at_threshold_excluded(self):
        # max_views=5 means strict less-than
        df = pd.DataFrame(
            [_activity_row(ancestors=["[adhoc]"], age_days=20, views_3m=5)]
        )
        result = select_soft_archive_candidates(df, [ADHOC_RULE])
        assert result == []

    def test_first_rule_wins_when_card_matches_multiple(self):
        # Card matches BOTH thematic and adhoc patterns
        df = pd.DataFrame(
            [
                _activity_row(
                    card_id=1,
                    ancestors=["[Adhoc] suivi par thématiques"],
                    age_days=120,
                    views_3m=0,
                    views_6m=0,
                )
            ]
        )
        # Thematic listed first → wins
        result = select_soft_archive_candidates(df, [THEMATIC_RULE, ADHOC_RULE])
        assert len(result) == 1
        assert result[0]["rule_name"] == "thematic"

        # Adhoc listed first → wins
        result = select_soft_archive_candidates(df, [ADHOC_RULE, THEMATIC_RULE])
        assert len(result) == 1
        assert result[0]["rule_name"] == "adhoc"

    def test_thematic_uses_6_month_window(self):
        # 6-month views > threshold but 3-month views = 0
        df = pd.DataFrame(
            [
                _activity_row(
                    card_id=1,
                    ancestors=["Suivi par thématiques"],
                    age_days=120,
                    views_3m=0,
                    views_6m=10,
                )
            ]
        )
        result = select_soft_archive_candidates(df, [THEMATIC_RULE])
        assert result == []

    def test_adhoc_uses_3_month_window(self):
        # 3-month views = 0 (below threshold) but 6-month views = 10
        df = pd.DataFrame(
            [
                _activity_row(
                    card_id=1,
                    ancestors=["[adhoc]"],
                    age_days=20,
                    views_3m=0,
                    views_6m=10,
                )
            ]
        )
        result = select_soft_archive_candidates(df, [ADHOC_RULE])
        assert len(result) == 1

    def test_missing_views_column_raises(self):
        df = pd.DataFrame([_activity_row()])
        df = df.drop(columns=["total_views_3_months"])
        with pytest.raises(ValueError, match="total_views_3_months"):
            select_soft_archive_candidates(df, [ADHOC_RULE])

    def test_recently_edited_card_excluded(self):
        # Card matches all other criteria but was edited 5 days ago, rule
        # requires 30+ days since update.
        rule = {**ADHOC_RULE, "min_days_since_update": 30}
        df = pd.DataFrame(
            [
                _activity_row(
                    ancestors=["[adhoc]"],
                    age_days=200,
                    update_age_days=5,
                    views_3m=0,
                )
            ]
        )
        assert select_soft_archive_candidates(df, [rule]) == []

    def test_old_edit_passes_update_filter(self):
        rule = {**ADHOC_RULE, "min_days_since_update": 30}
        df = pd.DataFrame(
            [
                _activity_row(
                    ancestors=["[adhoc]"],
                    age_days=200,
                    update_age_days=200,
                    views_3m=0,
                )
            ]
        )
        result = select_soft_archive_candidates(df, [rule])
        assert len(result) == 1

    def test_min_days_since_update_optional(self):
        # No min_days_since_update key → no edit-recency filter applied.
        df = pd.DataFrame(
            [_activity_row(ancestors=["[adhoc]"], age_days=200, update_age_days=1)]
        )
        assert len(select_soft_archive_candidates(df, [ADHOC_RULE])) == 1

    def test_missing_card_update_date_with_filter_raises(self):
        rule = {**ADHOC_RULE, "min_days_since_update": 30}
        df = pd.DataFrame([_activity_row(ancestors=["[adhoc]"])])
        df = df.drop(columns=["card_update_date"])
        with pytest.raises(ValueError, match="card_update_date"):
            select_soft_archive_candidates(df, [rule])

    def test_missing_ancestor_column_raises_clear_error(self):
        df = pd.DataFrame([_activity_row()])
        df = df.drop(columns=["ancestor_collection_names"])
        with pytest.raises(ValueError, match="ancestor_collection_names"):
            select_soft_archive_candidates(df, [ADHOC_RULE])

    def test_excluded_ids_are_skipped(self):
        df = pd.DataFrame(
            [
                _activity_row(card_id=1, ancestors=["[adhoc]"]),
                _activity_row(card_id=2, ancestors=["[adhoc]"]),
            ]
        )
        result = select_soft_archive_candidates(df, [ADHOC_RULE], excluded_ids={2})
        assert [c["id"] for c in result] == [1]

    def test_excluded_ids_none_passes_everything(self):
        df = pd.DataFrame([_activity_row(card_id=1, ancestors=["[adhoc]"])])
        assert (
            len(select_soft_archive_candidates(df, [ADHOC_RULE], excluded_ids=None))
            == 1
        )

    def test_handles_none_ancestors(self):
        row = _activity_row()
        row["ancestor_collection_names"] = None
        df = pd.DataFrame([row])
        result = select_soft_archive_candidates(df, [ADHOC_RULE])
        assert result == []


class TestMoveToArchive:
    def _card(self, name="My Card"):
        return {
            "id": 42,
            "name": name,
            "collection_id": 10,
            "rule_name": "adhoc",
            "last_execution_date": "2024-01-01",
            "last_execution_context": "api",
        }

    def test_rename_archive_object_adds_prefix(self, metabase):
        m = MoveToArchive(self._card("My Card"), 99, metabase)
        m.rename_archive_object()
        metabase.put_card.assert_called_once_with(42, {"name": "[Archive] - My Card"})

    def test_rename_archive_object_preserves_existing(self, metabase):
        m = MoveToArchive(self._card("[Archive] - My Card"), 99, metabase)
        m.rename_archive_object()
        metabase.put_card.assert_called_once_with(42, {"name": "[Archive] - My Card"})

    def test_rename_archive_object_case_insensitive(self, metabase):
        m = MoveToArchive(self._card("Old ARCHIVE card"), 99, metabase)
        m.rename_archive_object()
        metabase.put_card.assert_called_once_with(42, {"name": "Old ARCHIVE card"})

    def test_move_object_success(self, metabase):
        metabase.update_card_collections.return_value = {"status": "ok"}
        m = MoveToArchive(self._card(), 99, metabase)
        log_entry = m.move_object()
        assert log_entry["status"] == "success"
        assert log_entry["id"] == 42
        assert log_entry["new_collection_id"] == 99
        assert log_entry["previous_collection_id"] == 10
        assert log_entry["parent_folder"] == "adhoc"
        # No diagnostics recorded on success.
        assert log_entry["error_reason"] is None
        assert log_entry["http_status"] is None

    def test_move_object_failure(self, metabase):
        metabase.update_card_collections.return_value = {"status": "error"}
        m = MoveToArchive(self._card(), 99, metabase)
        log_entry = m.move_object()
        assert log_entry["status"] == "error"
        assert log_entry["error_reason"] is not None

    def test_move_object_json_rejection_records_reason(self, metabase):
        # A Metabase JSON error has no "status" key -> null status, but the
        # cause is captured in error_reason for BQ-side diagnosis.
        metabase.update_card_collections.return_value = {
            "cause": "Uses content that is not remote synced.",
            "data": {"status-code": 400},
        }
        m = MoveToArchive(self._card(), 99, metabase)
        log_entry = m.move_object()
        assert log_entry["status"] is None
        assert "not remote synced" in log_entry["error_reason"]

    def test_move_object_null_status_captures_http_body(self, metabase):
        # Non-JSON response (e.g. IAP login-page redirect) surfaced by
        # update_card_collections as {"status": None, "http_status", "body"}.
        metabase.update_card_collections.return_value = {
            "status": None,
            "http_status": 302,
            "body": "<html>Sign in with Google</html>",
        }
        m = MoveToArchive(self._card(), 99, metabase)
        log_entry = m.move_object()
        assert log_entry["status"] is None
        assert log_entry["http_status"] == 302
        assert "Sign in" in log_entry["error_reason"]

    def test_move_object_uses_archive_collection_id(self, metabase):
        metabase.update_card_collections.return_value = {"status": "ok"}
        m = MoveToArchive(self._card(), 717, metabase)
        m.move_object()
        metabase.update_card_collections.assert_called_once_with([42], 717)

    def test_failure_logs_summary_not_full_response(self, caplog, metabase):
        # A real Metabase remote-sync rejection — long Clojure stacktrace.
        metabase.update_card_collections.return_value = {
            "via": [
                {
                    "type": "clojure.lang.ExceptionInfo",
                    "message": "Uses content that is not remote synced.",
                }
            ],
            "trace": ["x"] * 200,  # very long; we don't want this in the log
            "cause": "Uses content that is not remote synced.",
            "data": {
                "non-remote-synced-models": [19627],
                "status-code": 400,
            },
            "message": "Uses content that is not remote synced.",
        }
        m = MoveToArchive(self._card(), 99, metabase)
        with caplog.at_level("ERROR"):
            m.move_object()
        # The cause and the actionable data are visible.
        assert "not remote synced" in caplog.text
        assert "non-remote-synced-models=[19627]" in caplog.text
        # The Clojure trace is NOT dumped at ERROR level.
        assert "trace" not in caplog.text

    def test_dry_run_skips_move(self, metabase, caplog):
        m = MoveToArchive(self._card(), 99, metabase, dry_run=True)
        with caplog.at_level("INFO"):
            log_entry = m.move_object()
        assert log_entry is None
        metabase.update_card_collections.assert_not_called()
        assert "[DRY-RUN]" in caplog.text

    def test_dry_run_skips_rename(self, metabase, caplog):
        m = MoveToArchive(self._card(), 99, metabase, dry_run=True)
        with caplog.at_level("INFO"):
            m.rename_archive_object()
        metabase.put_card.assert_not_called()
        assert "[DRY-RUN]" in caplog.text


class TestSummarizeMetabaseError:
    def test_remote_sync_failure(self):
        result = {
            "cause": "Uses content that is not remote synced.",
            "data": {"non-remote-synced-models": [1, 2], "status-code": 400},
        }
        msg = _summarize_metabase_error(result)
        assert "not remote synced" in msg
        assert "status-code=400" in msg
        assert "non-remote-synced-models=[1, 2]" in msg

    def test_via_message_fallback(self):
        # No top-level cause/message; pull from via[0].message.
        result = {
            "via": [{"message": "boom"}],
            "data": {"status-code": 500},
        }
        assert "boom" in _summarize_metabase_error(result)

    def test_body_fallback_for_non_json_response(self):
        # Non-JSON branch of update_card_collections (IAP redirect etc.).
        result = {"status": None, "http_status": 302, "body": "<html>login</html>"}
        assert "login" in _summarize_metabase_error(result)

    def test_unknown_shape(self):
        assert _summarize_metabase_error({}) == "unknown error"

    def test_non_dict_input(self):
        assert "fail" in _summarize_metabase_error("fail")


class TestFirstAncestor:
    def test_list(self):
        assert _first_ancestor(["3. adhoc", "team A"]) == "3. adhoc"

    def test_tuple(self):
        assert _first_ancestor(("1. external",)) == "1. external"

    def test_numpy_array(self):
        # pd.read_gbq returns ARRAY columns as numpy.ndarray.
        assert _first_ancestor(np.array(["3. adhoc", "team A"])) == "3. adhoc"

    def test_none(self):
        assert _first_ancestor(None) == "(no ancestor)"

    def test_empty_list(self):
        assert _first_ancestor([]) == "(no ancestor)"

    def test_empty_array(self):
        assert _first_ancestor(np.array([], dtype=object)) == "(no ancestor)"

    def test_falsy_first(self):
        assert _first_ancestor([""]) == "(no ancestor)"


class TestComputeArchiveStats:
    @patch("domain.archiving.pd.read_gbq")
    def test_no_soft_rules_no_hard_config(self, mock_read_gbq):
        mock_read_gbq.return_value = pd.DataFrame(
            {"scope": ["global", "personal"], "cards": [100, 20]}
        )
        stats = compute_archive_stats(None, {"empty_cleanup": {}})
        assert stats["soft_total"] == 0
        assert stats["hard_total"] == 0
        assert list(stats["scope_breakdown"]["scope"]) == ["global", "personal"]
        assert stats["cleanup_root_collection_ids"] == []

    @patch("domain.archiving.pd.read_gbq")
    def test_soft_breakdown_by_rule_and_top_folder(self, mock_read_gbq):
        # First read_gbq is the scope query.
        mock_read_gbq.return_value = pd.DataFrame({"scope": ["global"], "cards": [100]})
        df = pd.DataFrame(
            [
                _activity_row(card_id=1, ancestors=["[adhoc]", "Team A"]),
                _activity_row(card_id=2, ancestors=["[adhoc]", "Team B"]),
                _activity_row(
                    card_id=3,
                    ancestors=["suivi par thématiques", "Sub"],
                    age_days=120,
                ),
            ]
        )
        config = {"soft_archive_rules": [THEMATIC_RULE, ADHOC_RULE]}
        stats = compute_archive_stats(df, config)

        assert stats["soft_total"] == 3
        # First-rule-wins: thematic gets card 3, adhoc gets 1 and 2.
        per_folder = stats["soft_by_folder"]
        assert set(per_folder["rule_name"]) == {"thematic", "adhoc"}
        adhoc_row = per_folder[per_folder["rule_name"] == "adhoc"].iloc[0]
        assert adhoc_row["top_folder"] == "[adhoc]"
        assert adhoc_row["cards"] == 2

    @patch("domain.archiving.pd.read_gbq")
    def test_excluded_ids_show_up_in_stats(self, mock_read_gbq):
        mock_read_gbq.return_value = pd.DataFrame({"scope": ["global"], "cards": [10]})
        df = pd.DataFrame(
            [
                _activity_row(card_id=1, ancestors=["[adhoc]"]),
                _activity_row(card_id=2, ancestors=["[adhoc]"]),
            ]
        )
        config = {"soft_archive_rules": [ADHOC_RULE]}
        stats = compute_archive_stats(df, config, excluded_ids={2})
        assert stats["soft_excluded"] == 1
        assert stats["soft_total"] == 1  # only card 1 remains

    @patch("domain.archiving.pd.read_gbq")
    def test_soft_breakdown_handles_numpy_array_ancestors(self, mock_read_gbq):
        # pd.read_gbq returns ARRAY<STRING> columns as numpy arrays.
        mock_read_gbq.return_value = pd.DataFrame({"scope": ["global"], "cards": [10]})
        df = pd.DataFrame(
            [
                {
                    "card_id": 1,
                    "card_name": "Card",
                    "card_collection_id": 10,
                    "ancestor_collection_names": np.array(["3. adhoc", "Team A"]),
                    "card_creation_date": pd.Timestamp.utcnow().tz_localize(None)
                    - pd.Timedelta(days=120),
                    "total_views_3_months": 0,
                    "total_views_6_months": 0,
                    "last_execution_date": pd.Timestamp("2024-01-01"),
                    "last_execution_context": "api",
                }
            ]
        )
        stats = compute_archive_stats(df, {"soft_archive_rules": [ADHOC_RULE]})
        per_folder = stats["soft_by_folder"]
        # Real folder name, not "(no ancestor)".
        assert per_folder.iloc[0]["top_folder"] == "3. adhoc"

    @patch("domain.archiving.pd.read_gbq")
    def test_hard_archive_breakdown(self, mock_read_gbq):
        # Two reads: scope, then hard breakdown.
        mock_read_gbq.side_effect = [
            pd.DataFrame({"scope": ["global"], "cards": [10]}),
            pd.DataFrame(
                {
                    "top_folder": ["1. external", "2. internal"],
                    "cards": [40, 5],
                }
            ),
        ]
        config = {
            "hard_archive_cards": {
                "name_pattern": r"^\[Archive\]",
                "min_days_since_update": 30,
                "max_cards": 50,
            }
        }
        stats = compute_archive_stats(None, config)
        assert stats["hard_total"] == 45
        assert list(stats["hard_by_folder"]["top_folder"]) == [
            "1. external",
            "2. internal",
        ]
        # Hard breakdown query must exclude personal collections AND cards
        # already hard-archived (so stats match the action query).
        hard_query = mock_read_gbq.call_args_list[1][0][0]
        assert "personal_owner_id IS NULL" in hard_query
        assert "already_hard_archived" in hard_query


class TestLogArchiveStats:
    def test_logs_each_section(self, caplog):
        stats = {
            "scope_breakdown": pd.DataFrame(
                {"scope": ["global", "personal"], "cards": [10, 2]}
            ),
            "soft_total": 3,
            "soft_excluded": 0,
            "soft_by_folder": pd.DataFrame(
                {
                    "rule_name": ["adhoc"],
                    "top_folder": ["[adhoc]"],
                    "cards": [3],
                }
            ),
            "hard_total": 12,
            "hard_by_folder": pd.DataFrame(
                {"top_folder": ["1. external"], "cards": [12]}
            ),
            "cleanup_root_collection_ids": [608, 610],
            "cleanup_min_age_days": 15,
        }
        with caplog.at_level("INFO"):
            log_archive_stats(stats)

        text = caplog.text
        assert "Cards in scope" in text
        assert "Soft-archive candidates: 3" in text
        assert "Hard-archive candidates: 12" in text
        assert "608" in text and "610" in text


class TestLoadRecentlyFailedCardIds:
    @patch("domain.archiving.pd.read_gbq")
    def test_returns_set_of_ids(self, mock_read_gbq):
        mock_read_gbq.return_value = pd.DataFrame({"id": [1, 2, 3]})
        result = load_recently_failed_card_ids(cooldown_days=30)
        assert result == {1, 2, 3}

    @patch("domain.archiving.pd.read_gbq")
    def test_empty_result(self, mock_read_gbq):
        mock_read_gbq.return_value = pd.DataFrame({"id": []})
        assert load_recently_failed_card_ids(cooldown_days=30) == set()

    @patch("domain.archiving.pd.read_gbq")
    def test_query_filters_failures_in_window(self, mock_read_gbq):
        mock_read_gbq.return_value = pd.DataFrame({"id": []})
        load_recently_failed_card_ids(cooldown_days=14)
        query = mock_read_gbq.call_args[0][0]
        assert "archiving_log" in query
        assert "object_type = 'card'" in query
        assert "interval 14 day" in query
        # Must include both NULL and non-success statuses.
        assert "COALESCE(status, '') != 'success'" in query

    def test_none_cooldown_skips_query(self):
        # No BQ call when cooldown disabled.
        with patch("domain.archiving.pd.read_gbq") as mock_read_gbq:
            assert load_recently_failed_card_ids(cooldown_days=None) == set()
            mock_read_gbq.assert_not_called()


class TestAppendArchivingLogs:
    @patch("domain.archiving.bigquery.Client")
    def test_writes_single_dataframe(self, mock_bq_client):
        log_entries = [
            {"id": 1, "status": "success"},
            {"id": 2, "status": "success"},
            {"id": 3, "status": "success"},
        ]
        append_archiving_logs(log_entries)
        load = mock_bq_client.return_value.load_table_from_dataframe
        load.assert_called_once()
        # Whole batch loaded in one job, reindexed to the canonical schema.
        df_loaded = load.call_args[0][0]
        assert len(df_loaded) == 3
        assert list(df_loaded.columns) == [f.name for f in ARCHIVING_LOG_SCHEMA]
        # ALLOW_FIELD_ADDITION lets new audit fields create their column.
        job_config = load.call_args[1]["job_config"]
        assert (
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            in job_config.schema_update_options
        )

    @patch("domain.archiving.bigquery.Client")
    def test_no_op_on_empty(self, mock_bq_client):
        append_archiving_logs([])
        mock_bq_client.return_value.load_table_from_dataframe.assert_not_called()


class TestHardArchiveStaleCards:
    @patch("domain.archiving.bigquery.Client")
    @patch("domain.archiving.pd.read_gbq")
    def test_archives_candidates(self, mock_read_gbq, mock_bq_client, metabase):
        mock_read_gbq.return_value = pd.DataFrame(
            {"id": [1, 2], "card_collection_id": [99, 99]}
        )
        metabase.get_cards.return_value = {"archived": False, "collection_id": 99}
        # Success returns the updated card with archived flipped to true.
        metabase.put_card.return_value = {"archived": True, "collection_id": 99}

        result = hard_archive_stale_cards(
            metabase,
            name_pattern=r"^\[Archive\]",
            min_days_since_update=60,
            max_cards=50,
        )

        assert result == [1, 2]
        assert metabase.put_card.call_count == 2
        metabase.put_card.assert_any_call(1, {"archived": True})
        metabase.put_card.assert_any_call(2, {"archived": True})
        # Single batched BQ append, not one per card.
        assert mock_bq_client.return_value.load_table_from_dataframe.call_count == 1

    @patch("domain.archiving.bigquery.Client")
    @patch("domain.archiving.pd.read_gbq")
    def test_failed_put_is_not_counted_as_archived(
        self, mock_read_gbq, mock_bq_client, metabase
    ):
        mock_read_gbq.return_value = pd.DataFrame(
            {"id": [1], "card_collection_id": [99]}
        )
        metabase.get_cards.return_value = {"archived": False, "collection_id": 99}
        # Remote-sync rejection: error body, no `archived: true`.
        metabase.put_card.return_value = {
            "message": "Utilise du contenu qui n'est pas synchronisé à distance.",
            "data": {"status-code": 400, "non-remote-synced-models": [15367]},
            "http_status": 400,
        }

        result = hard_archive_stale_cards(
            metabase,
            name_pattern=r"^\[Archive\]",
            min_days_since_update=60,
            max_cards=50,
        )

        # Not a fake success — excluded from the result and logged as failed so
        # the cooldown skips it next run instead of the dedup CTE dropping it.
        assert result == []
        mock_bq_client.return_value.load_table_from_dataframe.assert_called_once()

    @patch("domain.archiving.bigquery.Client")
    @patch("domain.archiving.pd.read_gbq")
    def test_skips_already_archived(self, mock_read_gbq, mock_bq_client, metabase):
        mock_read_gbq.return_value = pd.DataFrame(
            {"id": [1], "card_collection_id": [99]}
        )
        metabase.get_cards.return_value = {"archived": True, "collection_id": 99}

        result = hard_archive_stale_cards(
            metabase,
            name_pattern=r"^\[Archive\]",
            min_days_since_update=60,
            max_cards=50,
        )

        # Not counted as newly archived and never re-PUT...
        assert result == []
        metabase.put_card.assert_not_called()
        # ...but the skip IS logged so the dedup CTE excludes it next run,
        # otherwise already-archived cards clog the LIMIT batch forever.
        load = mock_bq_client.return_value.load_table_from_dataframe
        load.assert_called_once()
        assert load.call_args[0][1] == "test-project.int_metabase_dev.archiving_log"

    @patch("domain.archiving.bigquery.Client")
    @patch("domain.archiving.pd.read_gbq")
    def test_dry_run_skips_mutations(self, mock_read_gbq, mock_bq_client, metabase):
        mock_read_gbq.return_value = pd.DataFrame(
            {"id": [1, 2], "card_collection_id": [99, 99]}
        )

        result = hard_archive_stale_cards(
            metabase,
            name_pattern=r"^\[Archive\]",
            min_days_since_update=60,
            max_cards=50,
            dry_run=True,
        )

        # All candidates listed, but no API calls or BQ writes.
        assert result == [1, 2]
        metabase.get_cards.assert_not_called()
        metabase.put_card.assert_not_called()
        mock_bq_client.return_value.load_table_from_dataframe.assert_not_called()

    @patch("domain.archiving.pd.read_gbq")
    def test_query_targets_raw_table(self, mock_read_gbq, metabase):
        mock_read_gbq.return_value = pd.DataFrame({"id": [], "card_collection_id": []})

        hard_archive_stale_cards(
            metabase,
            name_pattern=r"^\[Archive\]",
            min_days_since_update=60,
            max_cards=25,
        )

        query = mock_read_gbq.call_args[0][0]
        assert "raw_dev.metabase_report_card" in query
        assert "interval 60 day" in query
        assert "LIMIT 25" in query
        assert "REGEXP_CONTAINS" in query
        assert "(?i)" in query
        # The regex must reach BigQuery un-doubled: r'(?i)^\[Archive\]'.
        assert r"^\[Archive\]" in query
        assert r"^\\[Archive\\]" not in query
        # Must exclude personal collections.
        assert "personal_owner_id IS NULL" in query
        assert "raw_dev.metabase_collection" in query
        # Must also exclude cards already hard-archived by previous runs,
        # otherwise the SQL surfaces them every day and we waste 50 API
        # GETs only to skip them all.
        assert "already_hard_archived" in query
        assert "int_metabase_dev.archiving_log" in query
        # Cooldown (default 30d) must also exclude recently-failed cards so the
        # LIMIT batch isn't consumed retrying the same remote-sync dead-ends.
        assert "recently_failed" in query
        assert "interval 30 day" in query

    @patch("domain.archiving.pd.read_gbq")
    def test_cooldown_disabled_omits_recently_failed(self, mock_read_gbq, metabase):
        mock_read_gbq.return_value = pd.DataFrame({"id": [], "card_collection_id": []})

        hard_archive_stale_cards(
            metabase,
            name_pattern=r"^\[Archive\]",
            min_days_since_update=60,
            max_cards=25,
            failure_cooldown_days=None,
        )

        query = mock_read_gbq.call_args[0][0]
        assert "recently_failed" not in query


class TestIsOldEnough:
    def test_none_returns_false(self):
        assert _is_old_enough(None, 15) is False

    def test_empty_string_returns_false(self):
        assert _is_old_enough("", 15) is False

    def test_old_enough(self):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()
        assert _is_old_enough(old, 15) is True

    def test_too_recent(self):
        recent = (pd.Timestamp.utcnow() - pd.Timedelta(days=5)).isoformat()
        assert _is_old_enough(recent, 15) is False


class TestArchiveDeadDashboards:
    @pytest.fixture(autouse=True)
    def _mock_gbq(self):
        # Real (non-dry-run) cleanup now appends an audit batch to BQ; keep the
        # load job off the wire and expose the client mock to the tests.
        with patch("domain.archiving.bigquery.Client") as mock_bq_client:
            self.mock_bq_client = mock_bq_client
            yield mock_bq_client

    def test_no_items(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        result = archive_dead_dashboards(metabase, [100], min_age_days=15)
        assert result == []

    def test_only_archived_candidates_are_persisted(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()
        recent = (pd.Timestamp.utcnow() - pd.Timedelta(days=5)).isoformat()
        recorded = []
        with patch(
            "domain.archiving._record_cleanup_archive",
            side_effect=lambda entries, *a, **kw: recorded.append((a, kw)),
        ):
            metabase.get_collection_children.return_value = {
                "data": [
                    {"model": "dashboard", "id": 5, "created_at": old},
                    {"model": "dashboard", "id": 6, "created_at": recent},
                ]
            }
            metabase.get_dashboards.return_value = {"ordered_cards": []}
            archive_dead_dashboards(metabase, [100], min_age_days=15)
        # Dead-and-old → archived (persisted). Dead-but-recent → kept, log-only:
        # NOT persisted, otherwise a daily re-scan rewrites it every run.
        archived_ids = [args[0] for args, _ in recorded]
        assert archived_ids == [5]

    def test_archives_old_dead_dashboard(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()
        metabase.get_collection_children.return_value = {
            "data": [{"model": "dashboard", "id": 5, "created_at": old}]
        }
        metabase.get_dashboards.return_value = {"ordered_cards": []}

        result = archive_dead_dashboards(metabase, [100], min_age_days=15)
        assert result == [5]
        metabase.put_dashboard.assert_called_once_with(5, {"archived": True})

    def test_skips_recent_dead_dashboard(self, metabase):
        recent = (pd.Timestamp.utcnow() - pd.Timedelta(days=5)).isoformat()
        metabase.get_collection_children.return_value = {
            "data": [{"model": "dashboard", "id": 5, "created_at": recent}]
        }
        metabase.get_dashboards.return_value = {"ordered_cards": []}

        result = archive_dead_dashboards(metabase, [100], min_age_days=15)
        assert result == []
        metabase.put_dashboard.assert_not_called()

    def test_skips_live_dashboard(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()
        metabase.get_collection_children.return_value = {
            "data": [{"model": "dashboard", "id": 5, "created_at": old}]
        }
        metabase.get_dashboards.return_value = {
            "ordered_cards": [{"card": {"archived": False}}]
        }

        result = archive_dead_dashboards(metabase, [100], min_age_days=15)
        assert result == []

    def test_recursive_scan(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()

        def side_effect(collection_id, **kwargs):
            if collection_id == 100:
                return {
                    "data": [
                        {"model": "collection", "id": 200},
                        {"model": "dashboard", "id": 5, "created_at": old},
                    ]
                }
            elif collection_id == 200:
                return {"data": [{"model": "dashboard", "id": 6, "created_at": old}]}
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        metabase.get_dashboards.return_value = {"ordered_cards": []}

        result = archive_dead_dashboards(metabase, [100], min_age_days=15)
        assert set(result) == {5, 6}

    def test_dashboard_created_at_fallback(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()
        # Listing omits created_at; full-object fetch supplies it.
        metabase.get_collection_children.return_value = {
            "data": [{"model": "dashboard", "id": 5}]
        }
        metabase.get_dashboards.return_value = {
            "created_at": old,
            "updated_at": old,
            "ordered_cards": [],
        }

        result = archive_dead_dashboards(metabase, [100], min_age_days=15)
        assert result == [5]
        metabase.put_dashboard.assert_called_once_with(5, {"archived": True})

    def test_recently_updated_dashboard_skipped(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=60)).isoformat()
        recent = (pd.Timestamp.utcnow() - pd.Timedelta(days=2)).isoformat()
        metabase.get_collection_children.return_value = {
            "data": [
                {
                    "model": "dashboard",
                    "id": 5,
                    "created_at": old,
                    "updated_at": recent,
                }
            ]
        }
        metabase.get_dashboards.return_value = {"ordered_cards": []}

        result = archive_dead_dashboards(
            metabase, [100], min_age_days=15, min_days_since_update=15
        )
        assert result == []
        metabase.put_dashboard.assert_not_called()

    def test_dry_run_skips_dashboard_archival(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()
        metabase.get_collection_children.return_value = {
            "data": [{"model": "dashboard", "id": 5, "created_at": old}]
        }
        metabase.get_dashboards.return_value = {"ordered_cards": []}

        result = archive_dead_dashboards(metabase, [100], min_age_days=15, dry_run=True)
        assert result == [5]
        metabase.put_dashboard.assert_not_called()


class TestIsDashboardDead:
    def test_empty_dashboard_is_dead(self, metabase):
        metabase.get_dashboards.return_value = {"dashcards": []}
        assert _is_dashboard_dead(metabase, 1) is True

    def test_no_keys_at_all_is_dead(self, metabase):
        metabase.get_dashboards.return_value = {}
        assert _is_dashboard_dead(metabase, 1) is True

    def test_all_archived_cards_is_dead(self, metabase):
        metabase.get_dashboards.return_value = {
            "dashcards": [
                {"card": {"archived": True}},
                {"card": {"archived": True}},
            ]
        }
        assert _is_dashboard_dead(metabase, 1) is True

    def test_some_active_cards_not_dead(self, metabase):
        metabase.get_dashboards.return_value = {
            "dashcards": [
                {"card": {"archived": True}},
                {"card": {"archived": False}},
            ]
        }
        assert _is_dashboard_dead(metabase, 1) is False

    def test_legacy_ordered_cards_key_supported(self, metabase):
        metabase.get_dashboards.return_value = {
            "ordered_cards": [{"card": {"archived": False}}]
        }
        assert _is_dashboard_dead(metabase, 1) is False

    def test_dashcards_takes_precedence_over_ordered_cards(self, metabase):
        # When both keys are present, prefer the modern one.
        metabase.get_dashboards.return_value = {
            "dashcards": [{"card": {"archived": False}}],
            "ordered_cards": [],
        }
        assert _is_dashboard_dead(metabase, 1) is False

    def test_virtual_card_counts_as_live_content(self, metabase):
        # Text/heading/link/iframe dashcards have no `card` field.
        metabase.get_dashboards.return_value = {
            "dashcards": [{"card": None, "visualization_settings": {"text": "hi"}}]
        }
        assert _is_dashboard_dead(metabase, 1) is False

    def test_mix_of_archived_card_and_virtual_card_is_live(self, metabase):
        metabase.get_dashboards.return_value = {
            "dashcards": [
                {"card": {"archived": True}},
                {"card": None},  # text block
            ]
        }
        assert _is_dashboard_dead(metabase, 1) is False


class TestIsCollectionEmpty:
    def test_empty(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        assert _is_collection_empty(metabase, 1) is True

    def test_not_empty(self, metabase):
        metabase.get_collection_children.return_value = {
            "data": [{"model": "card", "id": 1}]
        }
        assert _is_collection_empty(metabase, 1) is False


class TestArchiveEmptyCollections:
    @pytest.fixture(autouse=True)
    def _mock_gbq(self):
        with patch("domain.archiving.bigquery.Client") as mock_bq_client:
            self.mock_bq_client = mock_bq_client
            yield mock_bq_client

    def test_no_empty_collections(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        result = archive_empty_collections(metabase, [100], min_age_days=15)
        assert result == []

    def test_only_archived_candidates_are_persisted(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()
        recent = (pd.Timestamp.utcnow() - pd.Timedelta(days=5)).isoformat()

        def side_effect(collection_id, **kwargs):
            if kwargs.get("models") == ["collection"]:
                if collection_id == 100:
                    return {
                        "data": [
                            {"model": "collection", "id": 200, "created_at": old},
                            {"model": "collection", "id": 201, "created_at": recent},
                        ]
                    }
                return {"data": []}  # 200 / 201 have no sub-collections
            return {"data": []}  # emptiness check: both empty

        metabase.get_collection_children.side_effect = side_effect

        recorded = []
        with patch(
            "domain.archiving._record_cleanup_archive",
            side_effect=lambda entries, *a, **kw: recorded.append((a, kw)),
        ):
            archive_empty_collections(metabase, [100], min_age_days=15)
        # Empty-and-old → archived (persisted). Empty-but-recent → kept, log-only:
        # NOT persisted, else a daily re-scan rewrites it every run.
        archived_ids = [args[0] for args, _ in recorded]
        assert archived_ids == [200]

    def test_archives_old_empty_leaf(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()

        def side_effect(collection_id, **kwargs):
            models = kwargs.get("models")
            if collection_id == 100 and models == ["collection"]:
                return {
                    "data": [
                        {"model": "collection", "id": 200, "created_at": old},
                    ]
                }
            if collection_id == 200 and models == ["collection"]:
                return {"data": []}
            # Empty-check (no models filter)
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        result = archive_empty_collections(metabase, [100], min_age_days=15)
        assert result == [200]
        metabase.put_collection.assert_called_once_with(200, {"archived": True})

    def test_skips_recent_empty_leaf(self, metabase):
        recent = (pd.Timestamp.utcnow() - pd.Timedelta(days=5)).isoformat()

        def side_effect(collection_id, **kwargs):
            models = kwargs.get("models")
            if collection_id == 100 and models == ["collection"]:
                return {
                    "data": [
                        {"model": "collection", "id": 200, "created_at": recent},
                    ]
                }
            if collection_id == 200 and models == ["collection"]:
                return {"data": []}
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        result = archive_empty_collections(metabase, [100], min_age_days=15)
        assert result == []
        metabase.put_collection.assert_not_called()

    def test_excludes_root_ids(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        archive_empty_collections(metabase, [100], min_age_days=15)
        # Root 100 itself should never be archived even if empty
        metabase.put_collection.assert_not_called()

    def test_excludes_explicit_ids_even_when_empty_and_old(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()

        def side_effect(collection_id, **kwargs):
            models = kwargs.get("models")
            if collection_id == 100 and models == ["collection"]:
                return {"data": [{"model": "collection", "id": 200, "created_at": old}]}
            if collection_id == 200 and models == ["collection"]:
                return {"data": []}
            return {"data": []}  # is_collection_empty: empty

        metabase.get_collection_children.side_effect = side_effect
        result = archive_empty_collections(
            metabase, [100], min_age_days=15, exclude_ids={100, 200}
        )
        assert result == []
        metabase.put_collection.assert_not_called()

    def test_uses_created_at_fallback_when_listing_omits_it(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()

        def side_effect(collection_id, **kwargs):
            models = kwargs.get("models")
            if collection_id == 100 and models == ["collection"]:
                return {"data": [{"model": "collection", "id": 200}]}
            if collection_id == 200 and models == ["collection"]:
                return {"data": []}
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        metabase.get_collections.return_value = {"created_at": old}

        result = archive_empty_collections(metabase, [100], min_age_days=15)
        assert result == [200]
        metabase.get_collections.assert_called_once_with(200)

    def test_recently_updated_collection_skipped(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=60)).isoformat()
        recent = (pd.Timestamp.utcnow() - pd.Timedelta(days=2)).isoformat()

        def side_effect(collection_id, **kwargs):
            models = kwargs.get("models")
            if collection_id == 100 and models == ["collection"]:
                return {
                    "data": [
                        {
                            "model": "collection",
                            "id": 200,
                            "created_at": old,
                            "updated_at": recent,
                        }
                    ]
                }
            if collection_id == 200 and models == ["collection"]:
                return {"data": []}
            return {"data": []}  # is_collection_empty: empty

        metabase.get_collection_children.side_effect = side_effect
        result = archive_empty_collections(
            metabase, [100], min_age_days=15, min_days_since_update=15
        )
        assert result == []
        metabase.put_collection.assert_not_called()

    def test_dry_run_skips_collection_archival(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()

        def side_effect(collection_id, **kwargs):
            models = kwargs.get("models")
            if collection_id == 100 and models == ["collection"]:
                return {"data": [{"model": "collection", "id": 200, "created_at": old}]}
            if collection_id == 200 and models == ["collection"]:
                return {"data": []}
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        result = archive_empty_collections(
            metabase, [100], min_age_days=15, dry_run=True
        )
        assert result == [200]
        metabase.put_collection.assert_not_called()

    def test_skips_non_empty_leaf(self, metabase):
        old = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).isoformat()

        def side_effect(collection_id, **kwargs):
            models = kwargs.get("models")
            if collection_id == 100 and models == ["collection"]:
                return {"data": [{"model": "collection", "id": 200, "created_at": old}]}
            if collection_id == 200 and models == ["collection"]:
                return {"data": []}
            # 200 is not empty (has a card)
            if collection_id == 200:
                return {"data": [{"model": "card", "id": 1}]}
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        result = archive_empty_collections(metabase, [100], min_age_days=15)
        assert result == []
        metabase.put_collection.assert_not_called()
