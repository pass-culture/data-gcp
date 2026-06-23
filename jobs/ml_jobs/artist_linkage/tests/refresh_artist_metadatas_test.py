import os
from unittest.mock import patch

import pandas as pd

from cli.refresh_artist_metadatas import main
from src.constants import (
    ARTIST_DESCRIPTION_KEY,
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_PRO_SEARCH_SCORE_KEY,
    IMG_KEY,
    WIKIDATA_ID_KEY,
    WIKIPEDIA_URL_KEY,
)


def test_refresh_artist_metadatas_matching(tmp_path):
    # 1. Setup mock data files
    artist_file = tmp_path / "applicative_artist.parquet"
    product_artist_link_file = tmp_path / "product_artist_link.parquet"
    product_file = tmp_path / "product.parquet"

    output_delta_artist = tmp_path / "delta_artist.parquet"
    output_delta_artist_alias = tmp_path / "delta_artist_alias.parquet"
    output_delta_product_link = tmp_path / "delta_product_link.parquet"

    # Mock applicative database artists:
    # id1 is already matched to Q1
    # id2 is unmatched (wikidata_id = None), name is Ed Sheeran
    artist_data = {
        ARTIST_ID_KEY: ["id1", "id2"],
        ARTIST_NAME_KEY: ["Artist One", "Ed Sheeran"],
        ARTIST_DESCRIPTION_KEY: [None, None],
        "wikidata_image_file_url": [None, None],
        WIKIDATA_ID_KEY: ["Q1", None],
        WIKIPEDIA_URL_KEY: [None, None],
        ARTIST_PRO_SEARCH_SCORE_KEY: [10.0, 20.0],
    }
    pd.DataFrame(artist_data).to_parquet(artist_file, index=False)

    # Mock product-artist links:
    # Link unmatched artist id2 to product 10
    link_data = {
        "offer_product_id": [10],
        ARTIST_ID_KEY: ["id2"],
        "artist_type": ["music"],
    }
    pd.DataFrame(link_data).to_parquet(product_artist_link_file, index=False)

    # Mock products:
    # Product 10 category is SPECTACLE, artist_name is Ed Sheeran
    product_data = {
        "offer_product_id": [10],
        ARTIST_NAME_KEY: ["Ed Sheeran"],
        "artist_type": ["music"],
        "offer_category_id": ["SPECTACLE"],
    }
    pd.DataFrame(product_data).to_parquet(product_file, index=False)

    # Mock wikidata dump:
    # Q1: already matched, gets updated description
    # Q2: matching Ed Sheeran (music=True, raw_alias=Ed Sheeran)
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
            "Refreshed Description One",
            "English singer-songwriter",
        ],
        "img": ["img1.jpg", "img2.jpg"],
        WIKIPEDIA_URL_KEY: [
            "https://wikipedia.org/wiki/Artist_One",
            "https://wikipedia.org/wiki/Ed_Sheeran",
        ],
    }

    # Mock environment variable to bypass ENV_SHORT_NAME check if needed, but we can set it to dev
    # which skips the sanity threshold checks.
    os.environ["ENV_SHORT_NAME"] = "dev"

    # 3. Call main using direct execution with patch for load_wikidata
    with patch("cli.refresh_artist_metadatas.load_wikidata") as mock_load:
        mock_load.return_value = pd.DataFrame(wiki_data)

        main(
            artist_file_path=str(artist_file),
            product_artist_link_filepath=str(product_artist_link_file),
            product_filepath=str(product_file),
            wiki_base_path=str(tmp_path),  # dummy path
            wiki_file_name="wiki.parquet",
            output_delta_artist_file_path=str(output_delta_artist),
            output_delta_artist_alias_file_path=str(output_delta_artist_alias),
            output_delta_product_artist_link_file_path=str(output_delta_product_link),
        )

    # 4. Load outputs and verify
    delta_artist_df = pd.read_parquet(output_delta_artist)
    delta_artist_alias_df = pd.read_parquet(output_delta_artist_alias)
    delta_product_link_df = pd.read_parquet(output_delta_product_link)

    # Output product link delta should be empty
    assert len(delta_product_link_df) == 0

    # Delta artist should contain two updates: Q1 and Q2
    assert len(delta_artist_df) == 2

    # Check already matched artist (id1)
    artist_1 = delta_artist_df[delta_artist_df[ARTIST_ID_KEY] == "id1"].iloc[0]
    assert artist_1[WIKIDATA_ID_KEY] == "Q1"
    assert artist_1[ARTIST_DESCRIPTION_KEY] == "Refreshed Description One"
    assert artist_1[IMG_KEY] == "img1.jpg"

    # Check newly matched artist (id2)
    artist_2 = delta_artist_df[delta_artist_df[ARTIST_ID_KEY] == "id2"].iloc[0]
    assert artist_2[WIKIDATA_ID_KEY] == "Q2"
    assert artist_2[ARTIST_DESCRIPTION_KEY] == "English singer-songwriter"
    assert artist_2[IMG_KEY] == "img2.jpg"

    # Delta artist alias should contain the new match for id2/Q2
    assert len(delta_artist_alias_df) == 0


def test_refresh_artist_metadatas_duplicate_wikidata_id_resolution(tmp_path):
    # 1. Setup mock data files
    artist_file = tmp_path / "applicative_artist.parquet"
    product_artist_link_file = tmp_path / "product_artist_link.parquet"
    product_file = tmp_path / "product.parquet"

    output_delta_artist = tmp_path / "delta_artist.parquet"
    output_delta_artist_alias = tmp_path / "delta_artist_alias.parquet"
    output_delta_product_link = tmp_path / "delta_product_link.parquet"

    # Three unmatched artists: id2 (Ed Sheeran), id3 (Edward Sheeran), id4 (Eddie Sheeran)
    artist_data = {
        ARTIST_ID_KEY: ["id2", "id3", "id4"],
        ARTIST_NAME_KEY: ["Ed Sheeran", "Edward Sheeran", "Eddie Sheeran"],
        ARTIST_DESCRIPTION_KEY: [None, None, None],
        "wikidata_image_file_url": [None, None, None],
        WIKIDATA_ID_KEY: [None, None, None],
        WIKIPEDIA_URL_KEY: [None, None, None],
        ARTIST_PRO_SEARCH_SCORE_KEY: [10.0, 50.0, 5.0],
    }
    pd.DataFrame(artist_data).to_parquet(artist_file, index=False)

    # Link unmatched artists to products to create different product counts:
    # id2 (Ed Sheeran) has 1 product
    # id3 (Edward Sheeran) has 3 products
    # id4 (Eddie Sheeran) has 0 products
    link_data = {
        "offer_product_id": [10, 20, 21, 22],
        ARTIST_ID_KEY: ["id2", "id3", "id3", "id3"],
        "artist_type": ["music", "music", "music", "music"],
    }
    pd.DataFrame(link_data).to_parquet(product_artist_link_file, index=False)

    product_data = {
        "offer_product_id": [10, 20, 21, 22],
        ARTIST_NAME_KEY: [
            "Ed Sheeran",
            "Edward Sheeran",
            "Edward Sheeran",
            "Edward Sheeran",
        ],
        "artist_type": ["music", "music", "music", "music"],
        "offer_category_id": ["SPECTACLE", "SPECTACLE", "SPECTACLE", "SPECTACLE"],
    }
    pd.DataFrame(product_data).to_parquet(product_file, index=False)

    # All three will match the same wikidata id Q2
    # Since Q2 matches all three, and they are duplicated on WIKIDATA_ID_KEY,
    # id3 (highest product count = 3) should be selected.
    wiki_data = {
        WIKIDATA_ID_KEY: ["Q2", "Q2", "Q2"],
        "artist_name": ["Ed Sheeran", "Edward Sheeran", "Eddie Sheeran"],
        "alias": ["Ed Sheeran", "Edward Sheeran", "Eddie Sheeran"],
        "raw_alias": ["Ed Sheeran", "Edward Sheeran", "Eddie Sheeran"],
        "matching_score": [1.0, 1.0, 1.0],
        "gkg": [200, 200, 200],
        "music": [True, True, True],
        "book": [False, False, False],
        "movie": [False, False, False],
        ARTIST_DESCRIPTION_KEY: [
            "Ed Sheeran singer",
            "Edward Sheeran singer",
            "Eddie Sheeran singer",
        ],
        "img": ["img2.jpg", "img3.jpg", "img4.jpg"],
        WIKIPEDIA_URL_KEY: [
            "https://wikipedia.org/wiki/Ed_Sheeran",
            "https://wikipedia.org/wiki/Edward_Sheeran",
            "https://wikipedia.org/wiki/Eddie_Sheeran",
        ],
    }

    os.environ["ENV_SHORT_NAME"] = "dev"

    with patch("cli.refresh_artist_metadatas.load_wikidata") as mock_load:
        mock_load.return_value = pd.DataFrame(wiki_data)

        main(
            artist_file_path=str(artist_file),
            product_artist_link_filepath=str(product_artist_link_file),
            product_filepath=str(product_file),
            wiki_base_path=str(tmp_path),
            wiki_file_name="wiki.parquet",
            output_delta_artist_file_path=str(output_delta_artist),
            output_delta_artist_alias_file_path=str(output_delta_artist_alias),
            output_delta_product_artist_link_file_path=str(output_delta_product_link),
        )

    delta_artist_df = pd.read_parquet(output_delta_artist)

    # Only id3 should be matched to Q2, id2 and id4 should not be in the delta
    assert len(delta_artist_df) == 1
    matched_artist = delta_artist_df.iloc[0]
    assert matched_artist[ARTIST_ID_KEY] == "id3"
    assert matched_artist[WIKIDATA_ID_KEY] == "Q2"
