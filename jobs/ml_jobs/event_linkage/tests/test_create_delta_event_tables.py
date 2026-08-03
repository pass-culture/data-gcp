import importlib

import pandas as pd
from typer.testing import CliRunner

from src.constants import (
    DESCRIPTION_SIMILARITY_COL,
    EVENT_SERIES_ID_COL,
    FULL_DESCRIPTION_SIMILARITY_COL,
    FULL_NAME_SIMILARITY_COL,
    IMAGE_SIMILARITY_COL,
    NAME_SIMILARITY_COL,
    OFFER_ID_COL,
    OFFER_SUBCATEGORY_ID_COL,
    PARTIAL_NAME_SIMILARITY_COL,
)
from src.interfaces import ActionType, CommentType

# Since the file name starts with a number, we import it using importlib.
create_delta_event_tables_module = importlib.import_module(
    "cli.3_create_delta_event_tables"
)
main_cli = create_delta_event_tables_module.app


def _make_raw_data_df():
    return pd.DataFrame(
        {
            OFFER_ID_COL: ["A", "B", "C", "D", "E", "F"],
            OFFER_SUBCATEGORY_ID_COL: [
                "CONCERT",
                "CONCERT",
                "CONCERT",
                "CONCERT",
                "CINEMA",
                "CINEMA",
            ],
            "offer_name": [
                "Concert Foo Live",
                "Concert Foo Live 2",
                "Concert Bar Live",
                "Concert Bar Live 2",
                "Movie X",
                "Movie X 2",
            ],
            "offer_description": [
                "Desc A",
                "Desc B",
                "Desc C",
                "Desc D",
                "Desc E",
                "Desc F",
            ],
            "image_url": [
                "url_a",
                "url_b",
                "url_c",
                "url_d",
                "url_e",
                "url_f",
            ],
        }
    )


def _make_similarities_df():
    # A-B: new cluster within CONCERT.
    # C-D: C is new, D is already linked to event S1 -> C should link to S1.
    # E-F: matches, but CINEMA is not in SUBCATEGORIES_TO_CLUSTERIZE and must
    #      be entirely ignored.
    rows = [
        ["A", "B"],
        ["C", "D"],
        ["E", "F"],
    ]
    return pd.DataFrame(
        rows, columns=[f"{OFFER_ID_COL}_1", f"{OFFER_ID_COL}_2"]
    ).assign(
        **{
            PARTIAL_NAME_SIMILARITY_COL: 80,
            NAME_SIMILARITY_COL: 95,
            FULL_NAME_SIMILARITY_COL: 90,
            DESCRIPTION_SIMILARITY_COL: 0,
            FULL_DESCRIPTION_SIMILARITY_COL: 0,
            IMAGE_SIMILARITY_COL: 0.0,
        }
    )


def _make_event_offer_link_df():
    return pd.DataFrame(
        [["S1", "D"], ["S2", "OLD_GONE"]],
        columns=[EVENT_SERIES_ID_COL, OFFER_ID_COL],
    )


def _run_cli(tmp_path, raw_data_df, similarities_df, event_offer_link_df, extra_args):
    offer_event_filepath = tmp_path / "offer_event.parquet"
    similarities_filepath = tmp_path / "similarities.parquet"
    event_series_offer_link_filepath = tmp_path / "event_series_offer_link.parquet"
    delta_events_filepath = tmp_path / "delta_events.parquet"
    delta_event_offer_link_filepath = tmp_path / "delta_event_offer_link.parquet"

    raw_data_df.to_parquet(offer_event_filepath)
    similarities_df.to_parquet(similarities_filepath)
    event_offer_link_df.to_parquet(event_series_offer_link_filepath)

    runner = CliRunner()
    result = runner.invoke(
        main_cli,
        [
            "--offer-event-filepath",
            str(offer_event_filepath),
            "--similarities-filepath",
            str(similarities_filepath),
            "--event-series-offer-link-filepath",
            str(event_series_offer_link_filepath),
            "--delta-events-filepath",
            str(delta_events_filepath),
            "--delta-event-offer-link-filepath",
            str(delta_event_offer_link_filepath),
            *extra_args,
        ],
    )

    assert result.exit_code == 0, f"CLI output: {result.output}"

    return (
        pd.read_parquet(delta_events_filepath),
        pd.read_parquet(delta_event_offer_link_filepath),
    )


class TestCreateDeltaEventTablesCLIIncremental:
    def test_links_clusters_and_removes_in_one_run(self, tmp_path):
        delta_events_df, delta_links_df = _run_cli(
            tmp_path,
            _make_raw_data_df(),
            _make_similarities_df(),
            _make_event_offer_link_df(),
            extra_args=[],
        )

        # Only one *new* event should be created, for the {A, B} cluster.
        # C-D does not create a new event since D is already linked (C is
        # linked to the existing event S1 instead), and E-F is ignored
        # because CINEMA is not in SUBCATEGORIES_TO_CLUSTERIZE.
        new_events = delta_events_df.loc[lambda df: df["action"] == ActionType.ADD]
        assert len(new_events) == 1
        assert new_events.iloc[0]["comment"] == CommentType.NEW_EVENT
        new_event_id = new_events.iloc[0]["event_id"]

        # S2 is removed since its only offer (OLD_GONE) disappeared from the
        # raw offers input. S1 survives since D is still active.
        removed_events = delta_events_df.loc[
            lambda df: df["action"] == ActionType.REMOVE
        ]
        assert set(removed_events["event_id"]) == {"S2"}
        assert (removed_events["comment"] == CommentType.REMOVED_EVENT).all()

        # A and B are linked to the new event.
        new_event_links = delta_links_df.loc[lambda df: df["event_id"] == new_event_id]
        assert set(new_event_links[OFFER_ID_COL]) == {"A", "B"}
        assert (new_event_links["action"] == ActionType.ADD).all()
        assert (new_event_links["comment"] == CommentType.NEW_EVENT).all()

        # C is linked to the pre-existing event S1 (not clustered with D).
        linked_to_existing = delta_links_df.loc[
            lambda df: df["comment"] == CommentType.LINKED_TO_EXISTING_EVENT
        ]
        assert len(linked_to_existing) == 1
        assert linked_to_existing.iloc[0]["offer_id"] == "C"
        assert linked_to_existing.iloc[0]["event_id"] == "S1"

        # OLD_GONE's link to S2 is removed.
        removed_links = delta_links_df.loc[lambda df: df["action"] == ActionType.REMOVE]
        assert set(removed_links[OFFER_ID_COL]) == {"OLD_GONE"}

        # Nothing from the CINEMA offers (E, F) appears anywhere in the output.
        assert not {"E", "F"} & set(delta_links_df[OFFER_ID_COL])


class TestCreateDeltaEventTablesCLIFromScratch:
    def test_from_scratch_removes_all_existing_events_and_reclusters(self, tmp_path):
        delta_events_df, delta_links_df = _run_cli(
            tmp_path,
            _make_raw_data_df(),
            _make_similarities_df(),
            _make_event_offer_link_df(),
            extra_args=["--from-scratch"],
        )

        # Every pre-existing event_series is flagged for removal with the
        # full_reset comment, regardless of whether its offers are still
        # active (D is still active but S1 is removed anyway).
        removed_events = delta_events_df.loc[
            lambda df: df["action"] == ActionType.REMOVE
        ]
        assert set(removed_events["event_id"]) == {"S1", "S2"}
        assert (removed_events["comment"] == CommentType.FULL_RESET).all()

        # C and D are no longer treated as already-linked, so they are
        # clustered together into a new event instead of C joining S1.
        new_events = delta_events_df.loc[lambda df: df["action"] == ActionType.ADD]
        new_event_clusters = [
            set(
                delta_links_df.loc[
                    lambda df, event_id=event_id: df["event_id"] == event_id
                ][OFFER_ID_COL]
            )
            for event_id in new_events["event_id"]
        ]
        assert {"A", "B"} in new_event_clusters
        assert {"C", "D"} in new_event_clusters

        # No offer is linked to an existing event in from-scratch mode.
        assert (delta_links_df["comment"] != CommentType.LINKED_TO_EXISTING_EVENT).all()


class TestCreateDeltaEventTablesCLINoClusterizableSubcategory:
    def test_does_not_crash_when_no_offer_is_in_a_clusterized_subcategory(
        self, tmp_path
    ):
        # Regression test: if every offer in the input belongs to a
        # subcategory that isn't in SUBCATEGORIES_TO_CLUSTERIZE, cluster_dfs
        # and existing_event_links_dfs both stay empty, which used to crash
        # `pd.concat` with "No objects to concatenate".
        raw_data_df = pd.DataFrame(
            {
                OFFER_ID_COL: ["E", "F"],
                OFFER_SUBCATEGORY_ID_COL: ["CINEMA", "CINEMA"],
                "offer_name": ["Movie X", "Movie X 2"],
                "offer_description": ["Desc E", "Desc F"],
                "image_url": ["url_e", "url_f"],
            }
        )
        similarities_df = pd.DataFrame(
            [["E", "F"]], columns=[f"{OFFER_ID_COL}_1", f"{OFFER_ID_COL}_2"]
        ).assign(
            **{
                PARTIAL_NAME_SIMILARITY_COL: 80,
                NAME_SIMILARITY_COL: 95,
                FULL_NAME_SIMILARITY_COL: 90,
                DESCRIPTION_SIMILARITY_COL: 0,
                FULL_DESCRIPTION_SIMILARITY_COL: 0,
                IMAGE_SIMILARITY_COL: 0.0,
            }
        )
        empty_event_offer_link_df = pd.DataFrame(
            columns=[EVENT_SERIES_ID_COL, OFFER_ID_COL]
        )

        delta_events_df, delta_links_df = _run_cli(
            tmp_path,
            raw_data_df,
            similarities_df,
            empty_event_offer_link_df,
            extra_args=[],
        )

        assert len(delta_events_df) == 0
        assert len(delta_links_df) == 0
