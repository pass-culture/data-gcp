import os
from unittest.mock import patch

import pandas as pd

from cli.linkage import link_new_products as main
from src.common.constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    WIKIDATA_ID_KEY,
    WIKIPEDIA_URL_KEY,
)
from src.linkage.constants import (
    APPLE_MUSIC_ID_KEY,
    ARTIST_DESCRIPTION_KEY,
    DEEZER_ID_KEY,
    GENIUS_ID_KEY,
    ISNI_ID_KEY,
    MUSIC_PLATFORM_IDS_KEYS,
    SOUNDCLOUD_ID_KEY,
    SPOTIFY_ID_KEY,
)


def test_link_new_products_to_artists(tmp_path):
    # 1. Setup mock data files
    artist_file = tmp_path / "applicative_artist.parquet"
    artist_music_platform_file = tmp_path / "artist_music_platform.parquet"
    product_artist_link_file = tmp_path / "product_artist_link.parquet"
    product_file = tmp_path / "product.parquet"

    output_delta_artist = tmp_path / "delta_artist.parquet"
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

    # Mock artist_music_platform table (empty — no platform IDs stored yet)
    pd.DataFrame(
        {ARTIST_ID_KEY: [], **{k: [] for k in MUSIC_PLATFORM_IDS_KEYS}}
    ).to_parquet(artist_music_platform_file, index=False)

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
        SPOTIFY_ID_KEY: [None, "6eUKZXaKkcviH0Ku9w2n3V"],
        ISNI_ID_KEY: [None, "0000000114431409"],
        APPLE_MUSIC_ID_KEY: [None, "183313439"],
        DEEZER_ID_KEY: [None, "384236"],
        GENIUS_ID_KEY: [None, "45789"],
        SOUNDCLOUD_ID_KEY: [None, "17954600"],
    }

    # Run in dev environment
    os.environ["ENV_SHORT_NAME"] = "dev"

    with patch("cli.linkage.load_wikidata") as mock_load:
        mock_load.return_value = pd.DataFrame(wiki_data)

        main(
            artist_filepath=str(artist_file),
            artist_music_platform_filepath=str(artist_music_platform_file),
            product_artist_link_filepath=str(product_artist_link_file),
            product_filepath=str(product_file),
            wiki_base_path=str(tmp_path),
            wiki_file_name="wiki.parquet",
            output_delta_artist_file_path=str(output_delta_artist),
            output_delta_product_artist_link_filepath=str(output_delta_product_link),
        )

    # 4. Load outputs and verify
    delta_artist_df = pd.read_parquet(output_delta_artist)
    delta_product_link_df = pd.read_parquet(output_delta_product_link)

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


def test_remediates_existing_book_links_without_recreating_artists(tmp_path):
    artist_file = tmp_path / "applicative_artist.parquet"
    artist_music_platform_file = tmp_path / "artist_music_platform.parquet"
    product_artist_link_file = tmp_path / "product_artist_link.parquet"
    product_file = tmp_path / "product.parquet"

    output_delta_artist = tmp_path / "delta_artist.parquet"
    output_delta_product_link = tmp_path / "delta_product_link.parquet"

    # Existing artists in database (including artists without wikidata_id):
    # - id_menard: translator wrongly linked to product 10
    # - id_rowling: real author, already linked to product 11 (no wikidata_id needed to match!)
    # - id_goscinny & id_uderzo: co-authors, product 12 was only linked to id_goscinny
    artist_data = {
        ARTIST_ID_KEY: ["id_menard", "id_rowling", "id_goscinny", "id_uderzo"],
        ARTIST_NAME_KEY: [
            "Jean-François Ménard",
            "J. K. Rowling",
            "René Goscinny",
            "Albert Uderzo",
        ],
        ARTIST_DESCRIPTION_KEY: [None, None, None, None],
        "wikidata_image_file_url": [None, None, None, None],
        WIKIDATA_ID_KEY: [None, None, None, None],
        WIKIPEDIA_URL_KEY: [None, None, None, None],
    }
    pd.DataFrame(artist_data).to_parquet(artist_file, index=False)

    pd.DataFrame(
        {ARTIST_ID_KEY: [], **{k: [] for k in MUSIC_PLATFORM_IDS_KEYS}}
    ).to_parquet(artist_music_platform_file, index=False)

    # Existing links before Titelive remediation:
    # - Product 10 (Harry Potter T1): wrongly linked to id_menard
    # - Product 11 (Harry Potter T2): already linked to id_rowling
    # - Product 12 (Astérix): only linked to id_goscinny
    # - Product 13 ( Uderzo solo book): linked to id_uderzo
    link_data = {
        "offer_product_id": [10, 11, 12, 13],
        ARTIST_ID_KEY: ["id_menard", "id_rowling", "id_goscinny", "id_uderzo"],
        "artist_type": ["author", "author", "author", "author"],
    }
    pd.DataFrame(link_data).to_parquet(product_artist_link_file, index=False)

    # Current products with clean Titelive authors:
    # - Product 10: now has j. k. rowling instead of Jean-François Ménard
    # - Product 11: still j. k. rowling
    # - Product 12: now has both rené goscinny and albert uderzo
    # - Product 13: still albert uderzo
    product_data = {
        "offer_product_id": [10, 11, 12, 12, 13],
        ARTIST_NAME_KEY: [
            "j. k. rowling",
            "j. k. rowling",
            "rené goscinny",
            "albert uderzo",
            "albert uderzo",
        ],
        "artist_type": ["author", "author", "author", "author", "author"],
        "offer_category_id": ["LIVRE", "LIVRE", "LIVRE", "LIVRE", "LIVRE"],
    }
    pd.DataFrame(product_data).to_parquet(product_file, index=False)

    wiki_data = {
        WIKIDATA_ID_KEY: ["Q1"],
        "artist_name": ["Some Other Artist"],
        "alias": ["Some Other Artist"],
        "raw_alias": ["Some Other Artist"],
        "matching_score": [1.0],
        "gkg": [100],
        "music": [False],
        "book": [True],
        "movie": [False],
        ARTIST_DESCRIPTION_KEY: ["Desc"],
        "img": [None],
        WIKIPEDIA_URL_KEY: [None],
        SPOTIFY_ID_KEY: [None],
        ISNI_ID_KEY: [None],
        APPLE_MUSIC_ID_KEY: [None],
        DEEZER_ID_KEY: [None],
        GENIUS_ID_KEY: [None],
        SOUNDCLOUD_ID_KEY: [None],
    }

    os.environ["ENV_SHORT_NAME"] = "dev"

    with patch("cli.linkage.load_wikidata") as mock_load:
        mock_load.return_value = pd.DataFrame(wiki_data)

        main(
            artist_filepath=str(artist_file),
            artist_music_platform_filepath=str(artist_music_platform_file),
            product_artist_link_filepath=str(product_artist_link_file),
            product_filepath=str(product_file),
            wiki_base_path=str(tmp_path),
            wiki_file_name="wiki.parquet",
            output_delta_artist_file_path=str(output_delta_artist),
            output_delta_product_artist_link_filepath=str(output_delta_product_link),
        )

    delta_artist_df = pd.read_parquet(output_delta_artist)
    delta_product_link_df = pd.read_parquet(output_delta_product_link)

    # Zero new artists should be created!
    assert delta_artist_df.empty

    # Product 10: remove id_menard, add id_rowling
    # Product 12: add id_uderzo (id_goscinny is kept untouched)
    # Products 11 and 13: untouched
    expected_links = {
        (10, "id_menard", "author", "remove"),
        (10, "id_rowling", "author", "add"),
        (12, "id_uderzo", "author", "add"),
    }
    actual_links = set(
        delta_product_link_df[
            ["offer_product_id", ARTIST_ID_KEY, "artist_type", "action"]
        ].itertuples(index=False, name=None)
    )
    assert actual_links == expected_links
