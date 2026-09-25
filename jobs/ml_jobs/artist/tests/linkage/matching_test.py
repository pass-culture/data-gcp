"""Tests for src/linkage/matching.py — no network calls needed.

Previously untested despite being the core dedup/Wikidata-matching logic (see
audit that motivated this file). Every function here is a pure pandas
transform, so these are plain input-DataFrame -> output-DataFrame assertions,
no mocking required.
"""

import pandas as pd
import pytest

from src.common.constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    WIKIDATA_ID_KEY,
)
from src.linkage.constants import (
    ARTIST_DESCRIPTION_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    ARTIST_TYPE_KEY,
    IMG_KEY,
    OFFER_CATEGORY_ID_KEY,
    POSTPROCESSED_ARTIST_NAME_KEY,
    PRODUCT_ID_KEY,
    Action,
    Comment,
)
from src.linkage.matching import (
    create_artists_tables,
    match_artist_on_offer_names,
    match_artists_with_wikidata,
    match_namesakes_per_category,
    match_per_category_no_namesakes,
    perform_wikidata_category_matching,
)


class TestMatchArtistOnOfferNames:
    def test_partitions_matched_and_unmatched_products(self):
        products_to_link_df = pd.DataFrame(
            {
                ARTIST_NAME_KEY: ["Daft Punk", "Some Unknown Band"],
                ARTIST_TYPE_KEY: ["music", "music"],
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE", "MUSIQUE_ENREGISTREE"],
            }
        )
        artist_alias_df = pd.DataFrame(
            {
                ARTIST_NAME_KEY: ["Daft Punk"],
                ARTIST_TYPE_KEY: ["music"],
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE"],
                ARTIST_ID_KEY: ["artist-1"],
            }
        )
        product_artist_link_df = pd.DataFrame(
            {ARTIST_ID_KEY: ["artist-1"], "offer_product_id": ["prod-1"]}
        )

        linked_df, unlinked_df = match_artist_on_offer_names(
            products_to_link_df, artist_alias_df, product_artist_link_df
        )

        assert len(linked_df) == 1
        assert linked_df.iloc[0][ARTIST_ID_KEY] == "artist-1"

        assert len(unlinked_df) == 1
        assert unlinked_df.iloc[0][ARTIST_NAME_KEY] == "Some Unknown Band"
        assert ARTIST_ID_KEY not in unlinked_df.columns


class TestCreateArtistsTables:
    def _exploded_artist_alias_df(self, artist_id="new-artist-id"):
        return pd.DataFrame(
            {
                ARTIST_ID_KEY: [artist_id],
                ARTIST_NAME_KEY: ["raw name"],
                ARTIST_DESCRIPTION_KEY: ["a description"],
                IMG_KEY: [None],
                WIKIDATA_ID_KEY: ["Q123"],
                "wikipedia_url": [None],
                "spotify_id": [None],
                "isni_id": [None],
                "apple_music_id": [None],
                "deezer_id": [None],
                "genius_id": [None],
                "soundcloud_id": [None],
                POSTPROCESSED_ARTIST_NAME_KEY: ["New Artist"],
                ARTIST_TYPE_KEY: ["music"],
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE"],
                ARTIST_NAME_TO_MATCH_KEY: ["new artist"],
            }
        )

    def _preproc_unlinked_products_df(self):
        return pd.DataFrame(
            {
                PRODUCT_ID_KEY: ["prod-2"],
                ARTIST_TYPE_KEY: ["music"],
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE"],
                ARTIST_NAME_TO_MATCH_KEY: ["new artist"],
            }
        )

    def test_default_args_create_new_artist_from_unlinked_product(self):
        delta_product_df, delta_artist_df = create_artists_tables(
            preproc_unlinked_products_df=self._preproc_unlinked_products_df(),
            exploded_artist_alias_df=self._exploded_artist_alias_df(),
        )

        assert len(delta_product_df) == 1
        product_row = delta_product_df.iloc[0]
        assert product_row[PRODUCT_ID_KEY] == "prod-2"
        assert product_row[ARTIST_ID_KEY] == "new-artist-id"
        assert product_row["action"] == Action.add
        assert product_row["comment"] == Comment.linked_to_new_artist

        assert len(delta_artist_df) == 1
        artist_row = delta_artist_df.iloc[0]
        # artist_name is overwritten by postprocessed_artist_name, not the raw one.
        assert artist_row[ARTIST_NAME_KEY] == "New Artist"
        assert artist_row[ARTIST_ID_KEY] == "new-artist-id"
        assert artist_row["action"] == Action.add
        assert artist_row["comment"] == Comment.new_artist

    def test_existing_artist_id_is_excluded_from_new_artists(self):
        artist_df = pd.DataFrame({ARTIST_ID_KEY: ["new-artist-id"]})

        _, delta_artist_df = create_artists_tables(
            preproc_unlinked_products_df=self._preproc_unlinked_products_df(),
            exploded_artist_alias_df=self._exploded_artist_alias_df(),
            artist_df=artist_df,
        )

        assert delta_artist_df.empty

    def test_products_to_remove_produce_remove_action(self):
        products_to_remove_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: ["prod-3"],
                ARTIST_ID_KEY: ["artist-old"],
                ARTIST_TYPE_KEY: ["music"],
            }
        )

        delta_product_df, _ = create_artists_tables(
            preproc_unlinked_products_df=pd.DataFrame(
                columns=[
                    PRODUCT_ID_KEY,
                    ARTIST_TYPE_KEY,
                    OFFER_CATEGORY_ID_KEY,
                    ARTIST_NAME_TO_MATCH_KEY,
                ]
            ),
            exploded_artist_alias_df=self._exploded_artist_alias_df(),
            products_to_remove_df=products_to_remove_df,
        )

        assert len(delta_product_df) == 1
        row = delta_product_df.iloc[0]
        assert row[PRODUCT_ID_KEY] == "prod-3"
        assert row["action"] == Action.remove
        assert row["comment"] == Comment.removed_linked


def _wiki_row(
    alias, wiki_artist_name, raw_alias, score, gkg, wikidata_id, **category_flags
):
    row = {
        "alias": alias,
        "wiki_artist_name": wiki_artist_name,
        "raw_alias": raw_alias,
        "matching_score": score,
        "gkg": gkg,
        WIKIDATA_ID_KEY: wikidata_id,
        "music": False,
        "book": False,
        "movie": False,
    }
    row.update(category_flags)
    return row


class TestMatchPerCategoryNoNamesakes:
    def test_filters_by_category_and_excludes_duplicated_aliases(self):
        artists_df = pd.DataFrame(
            {
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE", "LIVRE"],
                "alias": ["daft punk", "victor hugo"],
                ARTIST_ID_KEY: ["a1", "a2"],
            }
        )
        wikidata_df = pd.DataFrame(
            [
                _wiki_row(
                    "daft punk", "Daft Punk", "Daft Punk", 1.0, 0, "Q1", music=True
                ),
                _wiki_row(
                    "victor hugo", "Victor Hugo", "Victor Hugo", 1.0, 0, "Q2", book=True
                ),
                _wiki_row(
                    "someone else",
                    "Someone Else",
                    "Someone Else",
                    1.0,
                    0,
                    "Q3",
                    movie=True,
                ),
            ]
        )

        result_df = match_per_category_no_namesakes(artists_df, wikidata_df)

        assert set(result_df["alias"]) == {"daft punk", "victor hugo"}
        daft_row = result_df.loc[result_df["alias"] == "daft punk"].iloc[0]
        assert daft_row[WIKIDATA_ID_KEY] == "Q1"

    def test_namesaked_alias_is_excluded(self):
        artists_df = pd.DataFrame(
            {
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE"],
                "alias": ["daft punk"],
                ARTIST_ID_KEY: ["a1"],
            }
        )
        wikidata_df = pd.DataFrame(
            [
                _wiki_row(
                    "daft punk", "Daft Punk", "Daft Punk", 1.0, 0, "Q1", music=True
                ),
                _wiki_row(
                    "daft punk",
                    "Daft Punk Tribute",
                    "Daft Punk",
                    1.0,
                    1,
                    "Q2",
                    music=True,
                ),
            ]
        )

        result_df = match_per_category_no_namesakes(artists_df, wikidata_df)

        # Both wiki candidates share the alias "daft punk", so neither counts as a
        # namesake-free match — the merge falls back to an unmatched (NaN) row.
        assert len(result_df) == 1
        assert pd.isna(result_df.iloc[0][WIKIDATA_ID_KEY])


class TestMatchNamesakesPerCategory:
    def test_picks_best_wiki_entry_by_score_then_gkg_tiebreak(self):
        artists_df = pd.DataFrame(
            {
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE", "LIVRE"],
                "alias": ["daft punk", "solo"],
                ARTIST_ID_KEY: ["a1", "a2"],
            }
        )
        wikidata_df = pd.DataFrame(
            [
                # Two namesake candidates for "daft punk": neither wiki_artist_name
                # matches raw_alias, so the +0.5 exact-match bonus applies to
                # neither and matching_score ties them — the higher "gkg" value
                # must win the tiebreak.
                _wiki_row(
                    "daft punk",
                    "Daft Punk Global",
                    "Daft Punk",
                    1.0,
                    0,
                    "Q1",
                    music=True,
                ),
                _wiki_row(
                    "daft punk",
                    "Daft Punk Tribute",
                    "Daft Punk",
                    1.0,
                    1,
                    "Q2",
                    music=True,
                ),
                # Only one candidate for "solo" — not a namesake, so excluded here.
                _wiki_row(
                    "solo", "Solo Artist", "Solo Artist", 1.0, 0, "Q3", book=True
                ),
            ]
        )

        result_df = match_namesakes_per_category(artists_df, wikidata_df)

        assert len(result_df) == 2
        daft_row = result_df.loc[result_df["alias"] == "daft punk"].iloc[0]
        assert daft_row[WIKIDATA_ID_KEY] == "Q2"
        assert daft_row["gkg"] == 1

        solo_row = result_df.loc[result_df["alias"] == "solo"].iloc[0]
        assert pd.isna(solo_row[WIKIDATA_ID_KEY])

    def test_exact_name_match_bonus_breaks_score_tie(self):
        artists_df = pd.DataFrame(
            {
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE"],
                "alias": ["daft punk"],
                ARTIST_ID_KEY: ["a1"],
            }
        )
        wikidata_df = pd.DataFrame(
            [
                # Same matching_score and gkg — only the raw_alias == wiki_artist_name
                # exact-match bonus (+0.5) should decide the winner.
                _wiki_row(
                    "daft punk", "Daft Punk", "Daft Punk", 1.0, 0, "Q1", music=True
                ),
                _wiki_row(
                    "daft punk",
                    "Daft Punk Tribute",
                    "Daft Punk",
                    1.0,
                    0,
                    "Q2",
                    music=True,
                ),
            ]
        )

        result_df = match_namesakes_per_category(artists_df, wikidata_df)

        assert result_df.iloc[0][WIKIDATA_ID_KEY] == "Q1"


class TestPerformWikidataCategoryMatching:
    def test_reconciles_namesake_and_no_namesake_matches(self):
        new_artist_clusters_df = pd.DataFrame(
            {
                ARTIST_NAME_TO_MATCH_KEY: ["daft punk", "solo artist"],
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE", "LIVRE"],
                ARTIST_ID_KEY: ["a1", "a2"],
            }
        )
        wiki_df = pd.DataFrame(
            {
                ARTIST_NAME_KEY: ["Daft Punk", "Daft Punk Cover Band", "Solo Artist"],
                "raw_alias": ["Daft Punk", "Daft Punk", "Solo Artist"],
                "matching_score": [2.0, 1.0, 1.0],
                "gkg": [1, 0, 0],
                "music": [True, True, False],
                "book": [False, False, True],
                "movie": [False, False, False],
                WIKIDATA_ID_KEY: ["Q1", "Q2", "Q3"],
            }
        )

        result_df = perform_wikidata_category_matching(new_artist_clusters_df, wiki_df)

        assert len(result_df) == 2
        namesake_row = result_df.loc[result_df[ARTIST_ID_KEY] == "a1"].iloc[0]
        assert namesake_row[WIKIDATA_ID_KEY] == "Q1"
        assert bool(namesake_row["has_namesake"]) is True

        no_namesake_row = result_df.loc[result_df[ARTIST_ID_KEY] == "a2"].iloc[0]
        assert no_namesake_row[WIKIDATA_ID_KEY] == "Q3"
        assert bool(no_namesake_row["has_namesake"]) is False


class TestMatchArtistsWithWikidata:
    def _new_artist_clusters_df(self):
        return pd.DataFrame(
            {
                ARTIST_NAME_TO_MATCH_KEY: ["daft punk", "brand new act"],
                OFFER_CATEGORY_ID_KEY: ["MUSIQUE_ENREGISTREE", "MUSIQUE_ENREGISTREE"],
                ARTIST_ID_KEY: ["a1", "a2"],
                "artist_name_set": [["Daft Punk"], ["Brand New Act"]],
            }
        )

    def _wiki_df(self):
        return pd.DataFrame(
            {
                ARTIST_NAME_KEY: ["Daft Punk"],
                "raw_alias": ["Daft Punk"],
                "matching_score": [2.0],
                "gkg": [1],
                "music": [True],
                "book": [False],
                "movie": [False],
                WIKIDATA_ID_KEY: ["Q1"],
            }
        )

    def test_reuses_existing_artist_id_for_a_known_wikidata_id(self):
        # Mirrors the shape cli/link_new_products_to_artists.py actually builds:
        # [ARTIST_ID_KEY, WIKIDATA_ID_KEY], artist "existing-artist" already known
        # under Q1.
        artist_with_wiki_ids_df = pd.DataFrame(
            {ARTIST_ID_KEY: ["existing-artist"], WIKIDATA_ID_KEY: ["Q1"]}
        )

        result_df = match_artists_with_wikidata(
            self._new_artist_clusters_df(), self._wiki_df(), artist_with_wiki_ids_df
        )

        daft_punk_rows = result_df.loc[result_df[ARTIST_NAME_KEY] == "Daft Punk"]
        assert (daft_punk_rows[ARTIST_ID_KEY] == "existing-artist").all()

    def test_unmatched_cluster_keeps_its_own_artist_id(self):
        artist_with_wiki_ids_df = pd.DataFrame(
            {ARTIST_ID_KEY: ["existing-artist"], WIKIDATA_ID_KEY: ["Q1"]}
        )

        result_df = match_artists_with_wikidata(
            self._new_artist_clusters_df(), self._wiki_df(), artist_with_wiki_ids_df
        )

        unmatched_rows = result_df.loc[result_df[ARTIST_NAME_KEY] == "Brand New Act"]
        assert (unmatched_rows[ARTIST_ID_KEY] == "a2").all()

    def test_default_none_artist_with_wiki_ids_df_raises_instead_of_working(self):
        """Documents a real bug, not an intentional contract: the docstring says
        "If none, we assume no existing artists are present" (i.e. None should be
        a valid, working input), but the None-default fallback builds
        pd.DataFrame(columns=[ARTIST_ID_KEY, ARTIST_WIKI_ID_KEY]) — using
        ARTIST_WIKI_ID_KEY ("artist_wiki_id") where the function's own body
        immediately does `artist_with_wiki_ids_df[WIKIDATA_ID_KEY]`
        ("wikidata_id"). That column doesn't exist, so this raises KeyError
        instead of matching anything.
        This never bites in production today only because
        cli/link_new_products_to_artists.py (matching.py's one caller) always
        builds its own correctly-shaped frame with a real WIKIDATA_ID_KEY column
        and never passes None. If this starts passing, the bug's been fixed —
        replace this test with one asserting correct behavior instead."""
        with pytest.raises(KeyError, match="wikidata_id"):
            match_artists_with_wikidata(
                self._new_artist_clusters_df(),
                self._wiki_df(),
                artist_with_wiki_ids_df=None,
            )
