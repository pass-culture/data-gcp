"""Tests for the word-order-insensitive artist name matching key.

Titelive lists contributors as "Lastname Firstname" (e.g. "Verdon John",
"Grafton Sue") while existing artists / Wikidata aliases are stored as
"Firstname Lastname". These tests make sure both forms are matched together.
"""

import pandas as pd
import pytest

from src.common.constants import ARTIST_ID_KEY, ARTIST_NAME_KEY, WIKIDATA_ID_KEY
from src.linkage.constants import (
    ARTIST_NAME_SORTED_TO_MATCH_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    ARTIST_TYPE_KEY,
    OFFER_CATEGORY_ID_KEY,
    PRODUCT_ID_KEY,
)
from src.linkage.matching import (
    create_artists_tables,
    match_artist_on_offer_names,
    perform_wikidata_category_matching,
)
from src.linkage.preprocessing_utils import (
    build_order_insensitive_name_key,
    prepare_artist_names_for_matching,
)
from src.linkage.product_linking import get_products_to_remove_and_link_df


class TestBuildOrderInsensitiveNameKey:
    @pytest.mark.parametrize(
        ("name_a", "name_b"),
        [
            ("verdon john", "john verdon"),
            ("grafton sue", "sue grafton"),
            ("clark mary higgins", "mary higgins clark"),
            ("charland jean-pierre", "jean pierre charland"),
            ("r. r. martin george", "george r. r. martin"),
        ],
    )
    def test_same_key_regardless_of_word_order(self, name_a, name_b):
        assert build_order_insensitive_name_key(
            name_a
        ) == build_order_insensitive_name_key(name_b)

    def test_tokens_are_sorted_and_lowercased(self):
        assert build_order_insensitive_name_key("Verdon  John") == "john verdon"

    @pytest.mark.parametrize("value", [None, "", "  ", "-.", float("nan")])
    def test_empty_or_invalid_returns_none(self, value):
        assert build_order_insensitive_name_key(value) is None

    def test_prepare_artist_names_for_matching_adds_sorted_key(self):
        df = pd.DataFrame({ARTIST_NAME_KEY: ["Verdon John", "John Verdon"]})

        result_df = prepare_artist_names_for_matching(df)

        assert result_df[ARTIST_NAME_SORTED_TO_MATCH_KEY].tolist() == [
            "john verdon",
            "john verdon",
        ]
        # The display-oriented key keeps the original word order.
        assert result_df[ARTIST_NAME_TO_MATCH_KEY].tolist() == [
            "verdon john",
            "john verdon",
        ]


class TestGetProductsToRemoveAndLinkWithInvertedNames:
    def test_existing_link_is_kept_when_product_name_is_inverted(self):
        products_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1],
                ARTIST_NAME_KEY: ["Verdon John"],
                ARTIST_TYPE_KEY: ["author"],
                OFFER_CATEGORY_ID_KEY: ["LIVRE"],
            }
        )
        product_artist_link_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1],
                ARTIST_ID_KEY: ["artist-john-verdon"],
                ARTIST_TYPE_KEY: ["author"],
            }
        )
        artist_df = pd.DataFrame(
            {
                ARTIST_ID_KEY: ["artist-john-verdon"],
                ARTIST_NAME_KEY: ["John Verdon"],
            }
        )

        products_to_remove_df, products_to_link_df = get_products_to_remove_and_link_df(
            products_df, product_artist_link_df, artist_df
        )

        assert products_to_remove_df.empty
        assert products_to_link_df.empty


class TestGetProductsToRemoveAndLinkWithWikidataAliases:
    @staticmethod
    def _inputs(product_artist_name: str):
        products_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1],
                ARTIST_NAME_KEY: [product_artist_name],
                ARTIST_TYPE_KEY: ["author"],
                OFFER_CATEGORY_ID_KEY: ["LIVRE"],
            }
        )
        product_artist_link_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1],
                ARTIST_ID_KEY: ["artist-dostoievski"],
                ARTIST_TYPE_KEY: ["author"],
            }
        )
        artist_df = pd.DataFrame(
            {
                ARTIST_ID_KEY: ["artist-dostoievski"],
                ARTIST_NAME_KEY: ["Fiodor Dostoïevski"],
                WIKIDATA_ID_KEY: ["Q991"],
            }
        )
        wiki_df = pd.DataFrame(
            {
                WIKIDATA_ID_KEY: ["Q991", "Q991"],
                "raw_alias": ["Fiodor Dostoïevski", "Fedor Dostoïevski"],
            }
        )
        return products_df, product_artist_link_df, artist_df, wiki_df

    def test_existing_link_is_kept_when_product_name_is_a_wikidata_alias(self):
        products_df, links_df, artist_df, wiki_df = self._inputs("Dostoïevski Fedor")

        products_to_remove_df, products_to_link_df = get_products_to_remove_and_link_df(
            products_df, links_df, artist_df, wiki_df=wiki_df
        )

        assert products_to_remove_df.empty
        assert products_to_link_df.empty

    def test_existing_link_is_removed_without_wikidata_aliases(self):
        products_df, links_df, artist_df, _ = self._inputs("Dostoïevski Fedor")

        products_to_remove_df, products_to_link_df = get_products_to_remove_and_link_df(
            products_df, links_df, artist_df
        )

        assert products_to_remove_df[ARTIST_ID_KEY].tolist() == ["artist-dostoievski"]
        assert len(products_to_link_df) == 1

    def test_existing_link_is_removed_when_product_name_is_not_an_alias(self):
        products_df, links_df, artist_df, wiki_df = self._inputs("Emily Brontë")

        products_to_remove_df, products_to_link_df = get_products_to_remove_and_link_df(
            products_df, links_df, artist_df, wiki_df=wiki_df
        )

        assert products_to_remove_df[ARTIST_ID_KEY].tolist() == ["artist-dostoievski"]
        assert products_to_link_df[ARTIST_NAME_KEY].tolist() == ["Emily Brontë"]


class TestMatchArtistOnOfferNamesWithInvertedNames:
    def test_inverted_name_matches_existing_artist_alias(self):
        products_to_link_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [2],
                ARTIST_NAME_KEY: ["Grafton Sue"],
                ARTIST_TYPE_KEY: ["author"],
                OFFER_CATEGORY_ID_KEY: ["LIVRE"],
            }
        )
        artist_alias_df = pd.DataFrame(
            {
                ARTIST_ID_KEY: ["artist-sue-grafton"],
                ARTIST_NAME_KEY: ["sue grafton"],
                ARTIST_TYPE_KEY: ["author"],
                OFFER_CATEGORY_ID_KEY: ["LIVRE"],
            }
        )
        product_artist_link_df = pd.DataFrame(
            {ARTIST_ID_KEY: ["artist-sue-grafton"], PRODUCT_ID_KEY: [1]}
        )

        linked_df, unlinked_df = match_artist_on_offer_names(
            products_to_link_df, artist_alias_df, product_artist_link_df
        )

        assert len(linked_df) == 1
        assert linked_df.iloc[0][ARTIST_ID_KEY] == "artist-sue-grafton"
        assert unlinked_df.empty


class TestWikidataMatchingWithInvertedNames:
    def test_inverted_name_matches_wikidata_alias(self):
        new_artist_clusters_df = pd.DataFrame(
            {
                ARTIST_NAME_TO_MATCH_KEY: ["grafton sue"],
                OFFER_CATEGORY_ID_KEY: ["LIVRE"],
                ARTIST_ID_KEY: ["a1"],
            }
        )
        wiki_df = pd.DataFrame(
            {
                ARTIST_NAME_KEY: ["Sue Grafton"],
                "raw_alias": ["Sue Grafton"],
                "matching_score": [1.0],
                "gkg": [1],
                "music": [False],
                "book": [True],
                "movie": [False],
                WIKIDATA_ID_KEY: ["Q234865"],
            }
        )

        result_df = perform_wikidata_category_matching(new_artist_clusters_df, wiki_df)

        assert len(result_df) == 1
        assert result_df.iloc[0][WIKIDATA_ID_KEY] == "Q234865"


class TestCreateArtistsTablesWithInvertedNames:
    def test_unlinked_product_is_linked_to_cluster_with_inverted_name(self):
        preproc_unlinked_products_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [3],
                ARTIST_TYPE_KEY: ["author"],
                OFFER_CATEGORY_ID_KEY: ["LIVRE"],
                ARTIST_NAME_TO_MATCH_KEY: ["verdon john"],
            }
        )
        exploded_artist_alias_df = pd.DataFrame(
            {
                ARTIST_ID_KEY: ["new-artist-id"],
                ARTIST_NAME_KEY: ["John Verdon"],
                "artist_description": [None],
                "img": [None],
                WIKIDATA_ID_KEY: ["Q5933680"],
                "wikipedia_url": [None],
                "spotify_id": [None],
                "isni_id": [None],
                "apple_music_id": [None],
                "deezer_id": [None],
                "genius_id": [None],
                "soundcloud_id": [None],
                "postprocessed_artist_name": ["John Verdon"],
                ARTIST_TYPE_KEY: ["author"],
                OFFER_CATEGORY_ID_KEY: ["LIVRE"],
                ARTIST_NAME_TO_MATCH_KEY: ["john verdon"],
            }
        )

        delta_product_df, _ = create_artists_tables(
            preproc_unlinked_products_df=preproc_unlinked_products_df,
            exploded_artist_alias_df=exploded_artist_alias_df,
        )

        assert len(delta_product_df) == 1
        assert delta_product_df.iloc[0][ARTIST_ID_KEY] == "new-artist-id"
