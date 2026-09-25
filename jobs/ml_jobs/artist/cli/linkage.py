import uuid

import gcsfs
import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import typer
from loguru import logger
from sentence_transformers import SentenceTransformer

from src.common.constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ENCODER_NAME,
    ENV_SHORT_NAME,
    HF_TOKEN_SECRET_NAME,
    WIKIDATA_ID_KEY,
)
from src.common.gcp import get_secret
from src.common.mlflow import connect_remote_mlflow, get_mlflow_experiment
from src.linkage.constants import (
    ACTION_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    ARTIST_TYPE_KEY,
    COMMENT_KEY,
    IMG_KEY,
    MUSIC_PLATFORM_IDS_KEYS,
    OFFER_CATEGORY_ID_KEY,
    OFFER_IS_SYNCHRONISED,
    OFFER_NAME_KEY,
    PRODUCT_ID_KEY,
    Action,
)
from src.linkage.deduplication import get_artists_to_merge, get_namesakes
from src.linkage.evaluation import (
    DATASET_NAME_KEY,
    RAW_ARTIST_NAME_KEY,
    TOTAL_BOOKING_COUNT_KEY,
    WIKI_MATCHED_PERC,
    WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC,
    WIKI_MATCHED_WEIGHTED_BY_PRODUCT_PERC,
    get_main_artist_per_dataset,
    get_matching_metrics_per_dataset,
    get_wiki_matching_metrics,
    project_linked_artists_on_test_sets,
)
from src.linkage.loading import load_wikidata
from src.linkage.matching import (
    create_artists_tables,
    match_artist_on_offer_names,
    match_artists_with_wikidata,
)
from src.linkage.metadata_refresh import (
    create_delta_df_for_metadata_refresh,
    match_unmatched_artists_with_wikidata,
    sanity_check_metadata_refresh,
)
from src.linkage.preprocessing_utils import (
    filter_products,
    preprocess_offer_name,
)
from src.linkage.product_linking import (
    build_artist_alias,
    get_products_to_remove_and_link_df,
    sanity_check_product_links,
)

app = typer.Typer()

METRICS_PER_DATASET_CSV_FILENAME = "metrics_per_dataset.csv"
METRICS_PER_DATASET_GRAPH_FILENAME = "metrics_per_dataset.png"
GLOBAL_METRICS_FILENAME = "global_metrics.csv"


def get_test_sets_df(test_set_dir: str) -> pd.DataFrame:
    fs = gcsfs.GCSFileSystem()
    GS_PREFIX = "gs://"
    PARQUET_EXTENSION = ".parquet"

    parquet_files = [
        GS_PREFIX + path
        for path in fs.glob(f"{test_set_dir}/**")
        if path.endswith(PARQUET_EXTENSION)
    ]

    return pd.concat(
        [
            pd.read_parquet(test_set).assign(source_file_path=test_set)
            for test_set in parquet_files
        ]
    )


@app.command("deduplicate")
def deduplicate(
    applicative_artist_filepath: str = typer.Option(),
    applicative_product_artist_link_filepath: str = typer.Option(),
    artist_score_filepath: str = typer.Option(),
    product_embeddings_filepath: str = typer.Option(),
    output_delta_artist_filepath: str = typer.Option(),
    output_delta_product_artist_link_filepath: str = typer.Option(),
) -> None:
    # 1. Load raw data
    applicative_artist_df = pd.read_parquet(applicative_artist_filepath)
    applicative_product_artist_link_df = pd.read_parquet(
        applicative_product_artist_link_filepath
    )
    artist_with_score_df = pd.read_parquet(artist_score_filepath)
    product_embeddings_df = pd.read_parquet(product_embeddings_filepath).rename(
        columns={"embeddings": "embedding"}
    )

    # 2. Preprocess data
    namesake_artist_df = get_namesakes(artist_with_score_df)
    artist_id_to_score = dict(
        artist_with_score_df.set_index(ARTIST_ID_KEY)["artist_raw_score"]
    )

    # 3. Find artists to merge
    artists_to_merge = get_artists_to_merge(
        namesake_artist_df=namesake_artist_df,
        product_embeddings_df=product_embeddings_df,
        encoder=SentenceTransformer(
            ENCODER_NAME, token=get_secret(HF_TOKEN_SECRET_NAME)
        ),
    )
    logger.info(f"Number of artist groups to merge: {len(artists_to_merge)}")

    # 4. Build one to one artist mapping
    artist_mapping = {}
    for artist_ids in artists_to_merge:
        sorted_artist_ids = sorted(
            artist_ids,
            key=lambda artist_id: artist_id_to_score[artist_id],
            reverse=True,
        )
        main_artist_id = sorted_artist_ids[0]
        for duplicate_artist_id in sorted_artist_ids[1:]:
            artist_mapping[duplicate_artist_id] = main_artist_id
    logger.info(f"Number of artists to be merged: {len(artist_mapping)}")

    # 5. Build delta dataframes
    delta_artist_df = applicative_artist_df.loc[
        lambda df: df[ARTIST_ID_KEY].isin(artist_mapping.keys())
    ].assign(
        **{
            ACTION_KEY: Action.remove,
            COMMENT_KEY: "merged into another artist",
        }
    )
    delta_product_artist_link_df = applicative_product_artist_link_df.loc[
        lambda df: df[ARTIST_ID_KEY].isin(artist_mapping.keys())
    ].assign(
        **{
            ARTIST_ID_KEY: lambda df: df[ARTIST_ID_KEY].map(artist_mapping),
            ACTION_KEY: Action.add,
            COMMENT_KEY: "linked to main artist after deduplication",
        }
    )
    logger.info(
        f"Number of product-artist links to be updated: {len(delta_product_artist_link_df)}"
    )

    # 6. Save results
    delta_artist_df.to_parquet(
        output_delta_artist_filepath,
        index=False,
    )
    delta_product_artist_link_df.to_parquet(
        output_delta_product_artist_link_filepath,
        index=False,
    )


@app.command("embed-offer-names")
def embed_offer_names(
    applicative_product_artist_link_filepath: str = typer.Option(),
    artist_score_filepath: str = typer.Option(),
    product_stats_filepath: str = typer.Option(),
    output_product_embeddings_filepath: str = typer.Option(),
) -> None:
    PROMPT_NAME = "STS"
    BATCH_SIZE = 256

    # 1. Load raw data
    artist_df = pd.read_parquet(artist_score_filepath)
    applicative_product_artist_link_df = pd.read_parquet(
        applicative_product_artist_link_filepath
    )
    product_stats_df = pd.read_parquet(product_stats_filepath).dropna(
        subset=[OFFER_NAME_KEY]
    )

    # 2. Preprocess data
    namesake_artist_df = get_namesakes(artist_df)
    products_of_namesake_artists_df = (
        applicative_product_artist_link_df.loc[
            lambda df: df.artist_id.isin(
                namesake_artist_df.explode("artist_id_list").artist_id_list
            )
        ]
        .merge(product_stats_df, on=PRODUCT_ID_KEY, how="inner")
        .assign(
            preprocessed_offer_name=lambda df: df[OFFER_NAME_KEY].map(
                preprocess_offer_name
            )
        )
        .drop_duplicates()
    )

    # 3. Encode offer names
    HF_TOKEN = get_secret(HF_TOKEN_SECRET_NAME)
    encoder = SentenceTransformer(ENCODER_NAME, token=HF_TOKEN)
    embedding_array = encoder.encode(
        products_of_namesake_artists_df.dropna(
            subset=[OFFER_NAME_KEY]
        ).offer_name.tolist(),
        prompt_name=PROMPT_NAME,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
    )

    # 4. Save results
    products_of_namesake_artists_df.assign(
        embedding=lambda df: list(embedding_array)
    ).to_parquet(
        output_product_embeddings_filepath,
        index=False,
    )


@app.command("link-new-products")
def link_new_products(
    # Input files
    artist_filepath: str = typer.Option(),
    artist_music_platform_filepath: str = typer.Option(),
    product_artist_link_filepath: str = typer.Option(),
    product_filepath: str = typer.Option(),
    wiki_base_path: str = typer.Option(),
    wiki_file_name: str = typer.Option(),
    # Output files
    output_delta_artist_file_path: str = typer.Option(),
    output_delta_product_artist_link_filepath: str = typer.Option(),
) -> None:
    # 1. Load data
    product_artist_link_df = pd.read_parquet(product_artist_link_filepath).astype(
        {PRODUCT_ID_KEY: int}
    )
    product_df = (
        pd.read_parquet(product_filepath)
        .astype({PRODUCT_ID_KEY: int})
        .pipe(filter_products)
    )
    artist_music_platform_df = pd.read_parquet(artist_music_platform_filepath).loc[
        :, [ARTIST_ID_KEY, *MUSIC_PLATFORM_IDS_KEYS]
    ]
    artist_df = pd.read_parquet(artist_filepath).merge(
        artist_music_platform_df, on=ARTIST_ID_KEY, how="left", validate="one_to_one"
    )
    artist_with_wiki_ids_df = artist_df.rename(
        columns={
            "wikidata_id": WIKIDATA_ID_KEY,
        }
    ).loc[
        lambda df: df[WIKIDATA_ID_KEY].notna(),
        [ARTIST_ID_KEY, WIKIDATA_ID_KEY],
    ]
    wiki_df = load_wikidata(
        wiki_base_path=wiki_base_path, wiki_file_name=wiki_file_name
    ).reset_index(drop=True)
    artist_alias_df = build_artist_alias(
        product_df=product_df,
        product_artist_link_df=product_artist_link_df,
        artist_df=artist_df,
    )

    # 2. Split products between to remove and to link
    products_to_remove_df, products_to_link_df = get_products_to_remove_and_link_df(
        product_df, product_artist_link_df
    )

    # 3. Match products to link with artists on both raw and preprocessed offer names
    preproc_linked_products_df, preproc_unlinked_products_df = (
        match_artist_on_offer_names(
            products_to_link_df=products_to_link_df,
            artist_alias_df=artist_alias_df,
            product_artist_link_df=product_artist_link_df,
        )
    )

    # 4. Create new artist clusters by offer_category and artist type
    new_artist_clusters_df = (
        preproc_unlinked_products_df.groupby(
            [OFFER_CATEGORY_ID_KEY, ARTIST_TYPE_KEY, ARTIST_NAME_TO_MATCH_KEY]
        )
        .agg(
            **{
                ARTIST_ID_KEY: (ARTIST_NAME_KEY, lambda x: str(uuid.uuid4())),
            },
            artist_name_set=(ARTIST_NAME_KEY, lambda x: set(x.unique())),
            artist_name_count=(ARTIST_NAME_KEY, "count"),
            artist_name_nunique=(ARTIST_NAME_KEY, "nunique"),
        )
        .reset_index()
    )
    logger.info(
        f"Created {len(new_artist_clusters_df)} new artist clusters from {len(preproc_unlinked_products_df)} unlinked products."
    )

    # 5. Match new artist clusters with existing artists on Wikidata
    exploded_artist_alias_df = match_artists_with_wikidata(
        new_artist_clusters_df=new_artist_clusters_df,
        wiki_df=wiki_df,
        artist_with_wiki_ids_df=artist_with_wiki_ids_df,
    )

    # 6. Create new artists and artist aliases
    delta_product_df, delta_artist_df = create_artists_tables(
        preproc_unlinked_products_df=preproc_unlinked_products_df,
        exploded_artist_alias_df=exploded_artist_alias_df,
        products_to_remove_df=products_to_remove_df,
        preproc_linked_products_df=preproc_linked_products_df,
        artist_df=artist_df,
    )

    # 7. Sanity check for consistency
    sanity_check_product_links(
        delta_product_df,
        delta_artist_df,
        artist_df,
    )

    # 8. Save files
    delta_artist_df.to_parquet(output_delta_artist_file_path, index=False)
    delta_product_df.to_parquet(output_delta_product_artist_link_filepath, index=False)


@app.command("refresh-metadata")
def refresh_metadata(
    # Input files
    artist_file_path: str = typer.Option(),
    artist_music_platform_file_path: str = typer.Option(),
    product_artist_link_filepath: str = typer.Option(),
    product_filepath: str = typer.Option(),
    wiki_base_path: str = typer.Option(),
    wiki_file_name: str = typer.Option(),
    # Output files
    output_delta_artist_file_path: str = typer.Option(),
    output_delta_product_artist_link_file_path: str = typer.Option(),
) -> None:
    """Refresh artist metadata from wikidata.

    This function orchestrates the complete metadata refresh process:
    1. Loads artist + artist_music_platform tables and merges them, then loads wikidata
    2. Matches unmatched artists with wikidata to find new matches
    3. Matches artists with wikidata to refresh metadata
    4. Creates delta dataframes for the update operation
    5. Performs sanity checks to ensure data quality
    6. Saves the delta dataframes for downstream processing
    """
    # 1. Load data
    logger.info("Loading artist data...")
    artist_music_platform_df = pd.read_parquet(artist_music_platform_file_path).loc[
        :, [ARTIST_ID_KEY, *MUSIC_PLATFORM_IDS_KEYS]
    ]
    applicative_artist_df = (
        pd.read_parquet(artist_file_path)
        .rename(
            columns={
                "wikidata_image_file_url": IMG_KEY,
                "wikidata_id": WIKIDATA_ID_KEY,
            }
        )
        .merge(
            artist_music_platform_df,
            on=ARTIST_ID_KEY,
            how="left",
            validate="one_to_one",
        )
    )
    artist_with_wikidata_ids_df = applicative_artist_df.loc[
        lambda df: df[WIKIDATA_ID_KEY].notna()
    ]
    wiki_df = load_wikidata(
        wiki_base_path=wiki_base_path, wiki_file_name=wiki_file_name
    ).reset_index(drop=True)
    logger.success("Artist data loaded successfully.")
    logger.info(
        f"Number of artists: {len(applicative_artist_df)}, Number of artists with wikidata ids: {len(artist_with_wikidata_ids_df)}, Number of wikidata entries: {len(wiki_df)}"
    )

    # 2. Match on wikidata to have fresh metadatas for already matched artists
    logger.info("Refreshing artist metadatas from wikidata...")
    refreshed_artists_df = artist_with_wikidata_ids_df.merge(
        wiki_df.drop(columns=["alias", "raw_alias"]).drop_duplicates(),
        how="inner",
        on=WIKIDATA_ID_KEY,
        suffixes=("_old", ""),
    )

    # 3. Match existing unmatched artists on Wikidata
    product_artist_link_df = pd.read_parquet(product_artist_link_filepath).astype(
        {PRODUCT_ID_KEY: int}
    )
    product_df = (
        pd.read_parquet(product_filepath)
        .astype({PRODUCT_ID_KEY: int})
        .pipe(filter_products)
    )
    newly_matched_artists_df = match_unmatched_artists_with_wikidata(
        applicative_artist_df=applicative_artist_df,
        artist_with_wikidata_ids_df=artist_with_wikidata_ids_df,
        product_artist_link_df=product_artist_link_df,
        product_df=product_df,
        wiki_df=wiki_df,
    )

    # 4. Refresh statistics
    artists_with_wiki_id = artist_with_wikidata_ids_df[WIKIDATA_ID_KEY].notna().sum()
    artists_matched_in_wiki = refreshed_artists_df[ARTIST_NAME_KEY].notna().sum()
    artists_with_wiki_id_no_match = artists_with_wiki_id - artists_matched_in_wiki
    logger.info(
        f"Artists with wikidata_id: {artists_with_wiki_id}, "
        f"Matched in wikidata: {artists_matched_in_wiki}, "
        f"With wikidata_id but no match: {artists_with_wiki_id_no_match}"
    )
    logger.success("Artist metadatas refreshed successfully.")

    # 5. Build delta artist dataframe
    logger.info("Building delta artist dataframe...")
    delta_product_df, delta_artist_df = create_delta_df_for_metadata_refresh(
        refreshed_artists_df=refreshed_artists_df,
        newly_matched_artists_df=newly_matched_artists_df,
    )

    logger.success("Delta artist dataframe built successfully.")
    logger.info(f"Number of artists to update: {len(delta_artist_df)}")

    # 6. Sanity check for consistency
    logger.info("Performing sanity checks...")
    sanity_check_metadata_refresh(
        delta_product_df=delta_product_df,
        delta_artist_df=delta_artist_df,
        applicative_artist_df=applicative_artist_df,
    )
    logger.success("Sanity checks passed successfully.")

    # 7. Save files
    logger.info("Saving delta dataframes...")
    logger.info(
        f"Saving delta artist dataframes to {output_delta_artist_file_path} and {output_delta_product_artist_link_file_path}."
    )
    delta_artist_df.to_parquet(output_delta_artist_file_path, index=False)
    delta_product_df.to_parquet(output_delta_product_artist_link_file_path, index=False)
    logger.success("Delta dataframes saved successfully.")


@app.command("evaluate")
def evaluate(
    products_to_link_file_path: str = typer.Option(),
    artists_file_path: str = typer.Option(),
    product_artist_link_file_path: str = typer.Option(),
    test_sets_dir: str = typer.Option(),
    experiment_name: str = typer.Option(),
) -> None:
    # 1. Load Data
    test_sets_df = get_test_sets_df(test_sets_dir).rename(
        columns={"is_synchronised": OFFER_IS_SYNCHRONISED}
    )
    products_to_link_df = (
        pd.read_parquet(products_to_link_file_path)
        .astype({PRODUCT_ID_KEY: int})
        .rename(columns={ARTIST_NAME_KEY: RAW_ARTIST_NAME_KEY})
    )
    artists_df = pd.read_parquet(artists_file_path)
    product_artist_link_df = pd.read_parquet(product_artist_link_file_path).astype(
        {PRODUCT_ID_KEY: int}
    )

    # 2. Rebuild products with artists metadata
    linked_products_df = products_to_link_df.merge(
        product_artist_link_df,
        how="left",
        on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY],
    ).merge(artists_df, how="left", on=ARTIST_ID_KEY)

    # Global Metrics
    artists_with_stats_df = (
        linked_products_df.groupby([ARTIST_ID_KEY, OFFER_CATEGORY_ID_KEY])
        .agg(
            total_product_count=(PRODUCT_ID_KEY, "nunique"),
            total_booking_count=(TOTAL_BOOKING_COUNT_KEY, "sum"),
            artist_name=(ARTIST_NAME_KEY, "first"),
            wikidata_id=(WIKIDATA_ID_KEY, "first"),
        )
        .reset_index()
    )
    global_wiki_matching_metrics_df = get_wiki_matching_metrics(artists_with_stats_df)

    # Test Set Metrics
    linked_products_on_test_sets_df = project_linked_artists_on_test_sets(
        linked_products_df=linked_products_df, test_sets_df=test_sets_df
    )

    if linked_products_on_test_sets_df.empty:
        if ENV_SHORT_NAME == "dev":
            logger.info(
                "No linked products found on test sets. This is normal for dev Environment."
            )
        else:
            raise ValueError(
                "No linked products found on test sets. Is normal for dev Environment but should not happen in production."
            )
        return

    main_artist_per_dataset = get_main_artist_per_dataset(
        linked_products_on_test_sets_df
    )

    metrics_per_dataset_df = get_matching_metrics_per_dataset(
        linked_products_on_test_sets_df=linked_products_on_test_sets_df,
        main_artist_per_dataset=main_artist_per_dataset,
    )

    # MLflow Logging
    connect_remote_mlflow()
    experiment = get_mlflow_experiment(experiment_name=experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id):
        # Log Dataset
        dataset = mlflow.data.from_pandas(
            linked_products_on_test_sets_df,
            name="linked_products_on_test_sets_df",
        )
        mlflow.log_input(dataset, context="evaluation")

        # Log Metrics
        metrics_per_dataset_df.to_csv(METRICS_PER_DATASET_CSV_FILENAME, index=False)
        global_wiki_matching_metrics_df.to_csv(GLOBAL_METRICS_FILENAME, index=False)
        mlflow.log_artifact(METRICS_PER_DATASET_CSV_FILENAME)
        mlflow.log_artifact(GLOBAL_METRICS_FILENAME)
        mlflow.log_metrics(
            {
                "precision_mean": metrics_per_dataset_df.precision.mean(),
                "precision_std": metrics_per_dataset_df.precision.std(),
                "recall_mean": metrics_per_dataset_df.recall.mean(),
                "recall_std": metrics_per_dataset_df.recall.std(),
                "f1_mean": metrics_per_dataset_df.f1.mean(),
                "f1_std": metrics_per_dataset_df.f1.std(),
                WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC: global_wiki_matching_metrics_df[
                    WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC
                ]["TOTAL"],
                WIKI_MATCHED_WEIGHTED_BY_PRODUCT_PERC: global_wiki_matching_metrics_df[
                    WIKI_MATCHED_WEIGHTED_BY_PRODUCT_PERC
                ]["TOTAL"],
                WIKI_MATCHED_PERC: global_wiki_matching_metrics_df["wiki_matched_perc"][
                    "TOTAL"
                ],
            }
        )

        # Create and Log Graph
        ax = metrics_per_dataset_df.plot.barh(
            x=DATASET_NAME_KEY, y=["precision", "recall", "f1"], rot=0, figsize=(8, 12)
        )
        ax.legend(loc="upper left")
        plt.tight_layout()
        plt.savefig(METRICS_PER_DATASET_GRAPH_FILENAME)
        mlflow.log_artifact(METRICS_PER_DATASET_GRAPH_FILENAME)


if __name__ == "__main__":
    app()
