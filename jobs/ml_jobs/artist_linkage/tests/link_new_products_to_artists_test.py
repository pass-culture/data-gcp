import os
from unittest.mock import patch

import pandas as pd

from cli.link_new_products_to_artists import main
from src.constants import (
    ARTIST_DESCRIPTION_KEY,
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    WIKIDATA_ID_KEY,
    WIKIPEDIA_URL_KEY,
)


def test_link_new_products_to_artists(tmp_path):
    # 1. Setup mock data files
    artist_file = tmp_path / "applicative_artist.parquet"
    product_artist_link_file = tmp_path / "product_artist_link.parquet"
    product_file = tmp_path / "product.parquet"

    output_delta_artist = tmp_path / "delta_artist.parquet"
    output_delta_artist_alias = tmp_path / "delta_artist_alias.parquet"
    output_delta_product_link = tmp_path / "delta_product_link.parquet"

    # Existing database artist:
    # id1 is already matched to Q1
    artist_data = {
        ARTIST_ID_KEY: ["id1"],
        ARTIST_NAME_KEY: ["Artist One"],
        ARTIST_DESCRIPTION_KEY: ["Description One"],
        "wikidata_image_file_url": [None],
        WIKIDATA_ID_KEY: ["Q1"],
        WIKIPEDIA_URL_KEY: [None],
    }
    pd.DataFrame(artist_data).to_parquet(artist_file, index=False)

    # Current links: product 1 linked to id1
    link_data = {
        "offer_product_id": [1],
        ARTIST_ID_KEY: ["id1"],
        "artist_type": ["music"],
    }
    pd.DataFrame(link_data).to_parquet(product_artist_link_file, index=False)

    # Products:
    # Product 1: already linked
    # Product 2: new product (Ed Sheeran), matches Wikidata Q2
    # Product 3: new product (Unknown Band), does not match Wikidata
    product_data = {
        "offer_product_id": [1, 2, 3],
        ARTIST_NAME_KEY: ["Artist One", "Ed Sheeran", "Unknown Band"],
        "artist_type": ["music", "music", "music"],
        "offer_category_id": ["SPECTACLE", "SPECTACLE", "SPECTACLE"],
    }
    pd.DataFrame(product_data).to_parquet(product_file, index=False)

    # Wikidata dump:
    # Q1: Artist One
    # Q2: Ed Sheeran
    wiki_data = {
        WIKIDATA_ID_KEY: ["Q1", "Q2"],
        "artist_name": ["Artist One", "Ed Sheeran"],
        "alias": ["Artist One", "Ed Sheeran"],
        "raw_alias": ["Artist One", "Ed Sheeran"],
        "matching_score": [1.0, 1.0],
        "gkg": [100, 200],
        "music": [True, True],
        "book": [False, False],
        "movie": [False, False],
        ARTIST_DESCRIPTION_KEY: [
            "Description One",
            "English singer-songwriter",
        ],
        "img": ["img1.jpg", "img2.jpg"],
        WIKIPEDIA_URL_KEY: [
            "https://wikipedia.org/wiki/Artist_One",
            "https://wikipedia.org/wiki/Ed_Sheeran",
        ],
    }

    # Run in dev environment
    os.environ["ENV_SHORT_NAME"] = "dev"

    with patch("cli.link_new_products_to_artists.load_wikidata") as mock_load:
        mock_load.return_value = pd.DataFrame(wiki_data)

        main(
            artist_filepath=str(artist_file),
            product_artist_link_filepath=str(product_artist_link_file),
            product_filepath=str(product_file),
            wiki_base_path=str(tmp_path),
            wiki_file_name="wiki.parquet",
            output_delta_artist_file_path=str(output_delta_artist),
            output_delta_artist_alias_file_path=str(output_delta_artist_alias),
            output_delta_product_artist_link_filepath=str(output_delta_product_link),
        )

    # 4. Load outputs and verify
    delta_artist_df = pd.read_parquet(output_delta_artist)
    delta_artist_alias_df = pd.read_parquet(output_delta_artist_alias)
    delta_product_link_df = pd.read_parquet(output_delta_product_link)

    # Verify artist aliases:
    assert len(delta_artist_alias_df) >= 2

    # Verify products:
    # 2 new products should be in delta_product_link_df (product 2 and product 3)
    assert len(delta_product_link_df) == 2
    assert set(delta_product_link_df["offer_product_id"]) == {2, 3}
    assert delta_product_link_df["artist_id"].isna().sum() == 0

    # Verify delta_artist:
    # Should contain Ed Sheeran (matched Q2) and Unknown Band (not matched to any wikidata_id)
    # Note: since Unknown Band is not matched to wikidata, does it get added to delta_artist?
    # Let's check: in create_artists_tables, delta_artist_df is generated from exploded_artist_alias_df
    # which is returned by match_artists_with_wikidata.
    # On master, match_artists_with_wikidata returned all clusters (matched and unmatched).
    # So both should be in delta_artist_df.
    assert len(delta_artist_df) == 2
    names = set(delta_artist_df[ARTIST_NAME_KEY])
    assert "Ed Sheeran" in names
    assert "Unknown Band" in names

    # Ed Sheeran should have wikidata_id Q2
    ed_sheeran = delta_artist_df[delta_artist_df[ARTIST_NAME_KEY] == "Ed Sheeran"].iloc[
        0
    ]
    assert ed_sheeran[WIKIDATA_ID_KEY] == "Q2"

    # Unknown Band should have NaN wikidata_id
    unknown_band = delta_artist_df[
        delta_artist_df[ARTIST_NAME_KEY] == "Unknown Band"
    ].iloc[0]
    assert pd.isna(unknown_band[WIKIDATA_ID_KEY])
