"""Tests for the remediation of existing product-artist links."""

import pandas as pd

from src.common.constants import ARTIST_ID_KEY, ARTIST_NAME_KEY, WIKIDATA_ID_KEY
from src.linkage.constants import (
    ACTION_KEY,
    ARTIST_TYPE_KEY,
    OFFER_CATEGORY_ID_KEY,
    PRODUCT_ID_KEY,
    Action,
)
from src.linkage.remediation import exclude_unchanged_links, get_links_to_remediate_df


class TestGetLinksToRemediateWithInvertedNames:
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

        products_to_remove_df, products_to_link_df = get_links_to_remediate_df(
            products_df, product_artist_link_df, artist_df
        )

        assert products_to_remove_df.empty
        assert products_to_link_df.empty


class TestGetLinksToRemediateWithWikidataAliases:
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

        products_to_remove_df, products_to_link_df = get_links_to_remediate_df(
            products_df, links_df, artist_df, wiki_df=wiki_df
        )

        assert products_to_remove_df.empty
        assert products_to_link_df.empty

    def test_existing_link_is_removed_without_wikidata_aliases(self):
        products_df, links_df, artist_df, _ = self._inputs("Dostoïevski Fedor")

        products_to_remove_df, products_to_link_df = get_links_to_remediate_df(
            products_df, links_df, artist_df
        )

        assert products_to_remove_df[ARTIST_ID_KEY].tolist() == ["artist-dostoievski"]
        assert len(products_to_link_df) == 1

    def test_existing_link_is_removed_when_product_name_is_not_an_alias(self):
        products_df, links_df, artist_df, wiki_df = self._inputs("Emily Brontë")

        products_to_remove_df, products_to_link_df = get_links_to_remediate_df(
            products_df, links_df, artist_df, wiki_df=wiki_df
        )

        assert products_to_remove_df[ARTIST_ID_KEY].tolist() == ["artist-dostoievski"]
        assert products_to_link_df[ARTIST_NAME_KEY].tolist() == ["Emily Brontë"]


class TestExcludeUnchangedLinks:
    def test_drops_link_removed_then_re_added_to_same_artist(self):
        product_artist_link_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1],
                ARTIST_ID_KEY: ["artist-a"],
                ARTIST_TYPE_KEY: ["author"],
            }
        )
        delta_product_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1, 1],
                ARTIST_ID_KEY: ["artist-a", "artist-a"],
                ARTIST_TYPE_KEY: ["author", "author"],
                ACTION_KEY: [Action.remove, Action.add],
            }
        )

        result_df = exclude_unchanged_links(delta_product_df, product_artist_link_df)

        assert result_df.empty

    def test_drops_add_of_already_existing_link(self):
        product_artist_link_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1],
                ARTIST_ID_KEY: ["artist-a"],
                ARTIST_TYPE_KEY: ["author"],
            }
        )
        delta_product_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1, 2],
                ARTIST_ID_KEY: ["artist-a", "artist-a"],
                ARTIST_TYPE_KEY: ["author", "author"],
                ACTION_KEY: [Action.add, Action.add],
            }
        )

        result_df = exclude_unchanged_links(delta_product_df, product_artist_link_df)

        assert result_df[PRODUCT_ID_KEY].tolist() == [2]

    def test_keeps_actual_replacement(self):
        product_artist_link_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1],
                ARTIST_ID_KEY: ["artist-a"],
                ARTIST_TYPE_KEY: ["author"],
            }
        )
        delta_product_df = pd.DataFrame(
            {
                PRODUCT_ID_KEY: [1, 1],
                ARTIST_ID_KEY: ["artist-a", "artist-b"],
                ARTIST_TYPE_KEY: ["author", "author"],
                ACTION_KEY: [Action.remove, Action.add],
            }
        )

        result_df = exclude_unchanged_links(delta_product_df, product_artist_link_df)

        assert set(
            zip(result_df[ARTIST_ID_KEY], result_df[ACTION_KEY], strict=True)
        ) == {
            ("artist-a", Action.remove),
            ("artist-b", Action.add),
        }
