import time

import lancedb
import numpy as np
import pandas as pd
import tqdm
import typer
from loguru import logger

from src.common.constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    WIKIDATA_ID_KEY,
)
from src.linkage.loading import load_wikidata
from src.similarity.biography_enrichment import (
    BIRTH_DATE_KEY,
    GENRES_KEY,
    LANGUAGES_SPOKEN_KEY,
    PROFESSIONS_KEY,
    get_enriched_artist_df,
)
from src.similarity.constants import (
    ARTIST_APP_SEARCH_SCORE_KEY,
    ARTIST_BIOGRAPHY_KEY,
    MEAN_TT_ITEM_EMBEDDING_KEY,
    WIKIDATA_IMAGE_FILE_URL_KEY,
    WIKIPEDIA_CONTENT_KEY,
)
from src.similarity.embedding import embed_artist_biographies
from src.similarity.llm import merge_biographies, summarize_biographies_with_llm
from src.similarity.llm_config import MAX_CONCURRENT_LLM_REQUESTS
from src.similarity.vector_search import (
    LANCEDB_PATH,
    LANCEDB_TABLE_NAME,
    NUM_PARTITIONS,
    RANK_KEY,
    SEARCH_METRIC,
    format_results_df,
    merge_search_results,
    perform_search,
)
from src.similarity.wikimedia_license import (
    get_image_license,
    remove_image_with_improper_license,
)
from src.similarity.wikimedia_transfer import (
    DE_DATALAKE_BUCKET_NAME,
    _get_gcs_client,
    _get_session,
    run_parallel_image_transfers,
)
from src.similarity.wikipedia_content import (
    BATCH_INDEX_COLUMN,
    LANGUAGE_COLUMN,
    PAGE_TITLE_COLUMN,
    extract_wikipedia_content_from_url,
    fetch_clean_content,
    get_artists_to_extract_wikipedia_content_filter,
)

app = typer.Typer()


@app.command("get-wikimedia-license")
def get_wikimedia_license(
    artists_matched_on_wikidata: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artists_df = pd.read_parquet(artists_matched_on_wikidata).rename(
        columns={"img": WIKIDATA_IMAGE_FILE_URL_KEY}
    )

    # Fetch the licenses from wikidata
    image_list = (
        artists_df[WIKIDATA_IMAGE_FILE_URL_KEY].dropna().drop_duplicates().tolist()
    )
    image_license_df = get_image_license(image_list)

    artists_with_licenses_df = artists_df.merge(
        image_license_df, how="left", on=WIKIDATA_IMAGE_FILE_URL_KEY
    ).pipe(remove_image_with_improper_license)

    artists_with_licenses_df.to_parquet(output_file_path)


@app.command("transfer-images")
def transfer_images(
    artists_matched_on_wikidata: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    """Transfer Wikimedia artist images to Google Cloud Storage.

    Reads a parquet file containing artist data with Wikidata matches,
    extracts unique image URLs, downloads them from Wikimedia, and uploads
    them to GCS. The results are merged back with the original data and
    saved to the output file.
    """
    # 1. Load Data
    artists_df = pd.read_parquet(artists_matched_on_wikidata)
    image_urls = artists_df[WIKIDATA_IMAGE_FILE_URL_KEY].dropna().unique().tolist()

    # 2. Setup sessions and clients
    session = _get_session()
    gcs_client = _get_gcs_client()
    bucket = gcs_client.bucket(DE_DATALAKE_BUCKET_NAME)

    # 3. Run transfers in parallel
    result_df = run_parallel_image_transfers(session, bucket, image_urls)

    # 4. Merge results and save output
    artists_df.merge(
        result_df, on=WIKIDATA_IMAGE_FILE_URL_KEY, how="left", validate="m:1"
    ).to_parquet(output_file_path)


@app.command("get-wikipedia-content")
def get_wikipedia_content(
    applicative_artist_file_path: str = typer.Option(),
    artists_matched_on_wikidata: str = typer.Option(),
    output_file_path: str = typer.Option(),
    *,
    extract_all_from_scratch: bool = typer.Option(False),
) -> None:
    # Load + Preprocess Data
    applicative_artists_df = pd.read_parquet(applicative_artist_file_path).assign(
        **{
            ARTIST_BIOGRAPHY_KEY: lambda df: df[ARTIST_BIOGRAPHY_KEY]
            if ARTIST_BIOGRAPHY_KEY in df.columns
            else pd.NA
        }
    )
    artists_df = pd.read_parquet(
        artists_matched_on_wikidata
    ).merge(
        applicative_artists_df[[ARTIST_ID_KEY, ARTIST_BIOGRAPHY_KEY]],
        on=[ARTIST_ID_KEY],
        how="left",
        validate="one_to_one",
    )  # Retrieve previously fetched biographies to avoid recomputing wikipedia content + subsequent LLM summarization

    # Prepare Data
    filters_series = get_artists_to_extract_wikipedia_content_filter(
        artists_df=artists_df, extract_all_from_scratch=extract_all_from_scratch
    )
    logger.info(f"{filters_series.sum()} artists with a Wikipedia URL to process.")
    artists_with_wikipedia_url_df = artists_df.loc[filters_series].pipe(
        extract_wikipedia_content_from_url
    )

    # Fetch the wikipedia page content from MediaWiki API
    results_df_list = []
    if len(artists_with_wikipedia_url_df) == 0:
        logger.warning("No artists with Wikipedia URL found. Exiting.")
        results_df_list.append(
            pd.DataFrame(
                columns=[
                    ARTIST_ID_KEY,
                    PAGE_TITLE_COLUMN,
                    LANGUAGE_COLUMN,
                    BATCH_INDEX_COLUMN,
                    WIKIPEDIA_CONTENT_KEY,
                ]
            )
        )
    else:
        for (language, batch_index), group in artists_with_wikipedia_url_df.groupby(
            [LANGUAGE_COLUMN, BATCH_INDEX_COLUMN]
        ):
            t0 = time.time()
            wikipedia_pages = group[PAGE_TITLE_COLUMN].to_list()
            logger.info(f"Processing language: {language}, batch: {batch_index}...")
            content_dict = fetch_clean_content(
                wikipedia_titles=wikipedia_pages, wikipedia_language=language
            )
            content_df = pd.DataFrame(
                {
                    PAGE_TITLE_COLUMN: list(content_dict.keys()),
                    WIKIPEDIA_CONTENT_KEY: list(content_dict.values()),
                    BATCH_INDEX_COLUMN: batch_index,
                    LANGUAGE_COLUMN: language,
                }
            ).merge(
                group[[PAGE_TITLE_COLUMN, ARTIST_ID_KEY]],
                on=PAGE_TITLE_COLUMN,
                how="left",
            )
            results_df_list.append(content_df)
            logger.success(
                f"...Fetched {len(content_dict)} pages for language: {language}, batch: {batch_index} in {time.time() - t0:.2f} seconds."
            )

    # Merge back the wikipedia content to the original dataframe
    artists_id_with_wikipedia_content_df = artists_with_wikipedia_url_df.merge(
        pd.concat(results_df_list, ignore_index=True),
        on=[ARTIST_ID_KEY, PAGE_TITLE_COLUMN, LANGUAGE_COLUMN, BATCH_INDEX_COLUMN],
        how="left",
    ).loc[:, [ARTIST_ID_KEY, WIKIPEDIA_CONTENT_KEY]]

    # Merge back to original dataframe and save
    artist_with_content_df = artists_df.merge(
        artists_id_with_wikipedia_content_df,
        on=[ARTIST_ID_KEY],
        how="left",
        validate="one_to_one",
    )
    # The default behavior of pandas merge is to use np.nan for missing values.
    # We want to replace these with None to match the rest of the pipeline.
    artist_with_content_df.where(pd.notnull(artist_with_content_df), None).to_parquet(
        output_file_path, index=False
    )


@app.command("summarize-biographies")
def summarize_biographies(
    artists_with_wikipedia_content: str = typer.Option(),
    output_file_path: str = typer.Option(),
    number_of_biographies_to_summarize: int = typer.Option(None),
    debug: bool = typer.Option(False),  # noqa: FBT001
) -> None:
    artists_df = pd.read_parquet(artists_with_wikipedia_content)

    # Prepare Data
    artists_to_summarize_df = artists_df.loc[
        lambda df: df[WIKIPEDIA_CONTENT_KEY].notna()
    ].loc[lambda df: df[ARTIST_NAME_KEY].notna()]

    # Predict only on few artists for testing and staging
    if number_of_biographies_to_summarize is not None:
        artists_to_summarize_df = artists_to_summarize_df.head(
            number_of_biographies_to_summarize
        )

    # Summarize biographies with LLM
    artists_with_biographies_df = summarize_biographies_with_llm(
        artists_to_summarize_df,
        max_concurrent=min(MAX_CONCURRENT_LLM_REQUESTS, len(artists_to_summarize_df)),
        debug=debug,
    )

    # Merge back the biographies to the original dataframe
    merge_biographies(artists_df, artists_with_biographies_df).to_parquet(
        output_file_path, index=False
    )


@app.command("encode-biographies")
def encode_biographies(
    artist_with_biography_file_path: str = typer.Option(),
    wiki_base_path: str = typer.Option(),
    wiki_file_name: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artists_df = pd.read_parquet(artist_with_biography_file_path)
    wikidata_df = (
        load_wikidata(wiki_base_path, wiki_file_name)
        .loc[
            :,
            [
                WIKIDATA_ID_KEY,
                GENRES_KEY,
                PROFESSIONS_KEY,
                LANGUAGES_SPOKEN_KEY,
                BIRTH_DATE_KEY,
            ],
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Enrich artist biographies with Wikidata information
    enriched_artist_df = get_enriched_artist_df(artists_df, wikidata_df)

    # Encode enriched artist biographies
    enriched_artist_df.assign(
        semantic_embedding=lambda df: embed_artist_biographies(
            df.enriched_artist_biography
        )
    ).to_parquet(output_file_path, index=False)


@app.command("create-similar-artist-parquet")
def create_similar_artist_parquet(
    artist_with_embeddings_file_path: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artist_df = pd.read_parquet(artist_with_embeddings_file_path).assign(
        mean_tt_item_embedding=lambda df: df[MEAN_TT_ITEM_EMBEDDING_KEY].apply(
            lambda x: x if isinstance(x, list | np.ndarray) and len(x) > 0 else None
        )
    )

    # Create lance tables
    logger.info("Creating LanceDB table and indexes...")
    db = lancedb.connect(LANCEDB_PATH)
    if LANCEDB_TABLE_NAME in db.list_tables().tables:
        db.drop_table(LANCEDB_TABLE_NAME)
    artist_table = db.create_table(LANCEDB_TABLE_NAME, artist_df)
    logger.info("LanceDB table created successfully.")

    # Create indexes for both embedding columns to speed up search
    logger.info("Creating indexes for semantic and item embeddings...")
    artist_table.create_index(
        vector_column_name="semantic_embedding",
        metric=SEARCH_METRIC,
        num_partitions=NUM_PARTITIONS,
    )
    artist_table.create_index(
        vector_column_name="mean_tt_item_embedding",
        metric=SEARCH_METRIC,
        num_partitions=NUM_PARTITIONS,
    )
    logger.info("Indexes created successfully.")

    # Perform search for each artist and combine results
    result_df_list = []
    logger.info("Performing similarity search for each artist...")
    for _, selected_artist_row in tqdm.tqdm(artist_df.iterrows(), total=len(artist_df)):
        semantic_df = perform_search(
            artist_table, selected_artist_row, "semantic_embedding"
        )
        if selected_artist_row["mean_tt_item_embedding"] is not None:
            item_df = perform_search(
                artist_table, selected_artist_row, "mean_tt_item_embedding"
            )
        else:
            item_df = pd.DataFrame(
                columns=[
                    ARTIST_ID_KEY,
                    ARTIST_NAME_KEY,
                    ARTIST_APP_SEARCH_SCORE_KEY,
                    ARTIST_BIOGRAPHY_KEY,
                    RANK_KEY,
                ]
            )

        results_df = merge_search_results(semantic_df=semantic_df, item_df=item_df)
        formatted_results_df = format_results_df(
            results_df=results_df,
            selected_artist_id=selected_artist_row[ARTIST_ID_KEY],
            selected_artist_name=selected_artist_row[ARTIST_NAME_KEY],
        )
        result_df_list.append(formatted_results_df)

    if len(result_df_list) == 0:
        raise ValueError("No results found for any artist. Exiting without saving.")

    logger.info("Similarity search completed. Saving results to Parquet...")
    pd.concat(result_df_list).loc[:, lambda df: df.columns.sort_values()].to_parquet(
        output_file_path, index=False
    )
    logger.info("Results saved to Parquet successfully.")


if __name__ == "__main__":
    app()
