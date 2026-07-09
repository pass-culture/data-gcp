import numpy as np
import pandas as pd
import rapidfuzz
import typer
from loguru import logger
from rapidfuzz import fuzz

from src.constants import (
    DESCRIPTION_SIMILARITY_COL,
    FULL_DESCRIPTION_SIMILARITY_COL,
    FULL_NAME_SIMILARITY_COL,
    IMAGE_EMBEDDING_COL,
    IMAGE_SIMILARITY_COL,
    MIN_DESCRIPTION_LENGTH,
    NAME_SIMILARITY_COL,
    OFFER_DESCRIPTION_COL,
    OFFER_ID_COL,
    OFFER_NAME_COL,
    OFFER_SUBCATEGORY_ID_COL,
    PARTIAL_NAME_SIMILARITY_COL,
    PARTIAL_NAME_SIMILARITY_THRESHOLD,
)
from src.preprocessing import (
    description_preprocessing,
    full_name_preprocessing,
    offer_name_preprocessing,
)

app = typer.Typer()


def compute_similarities(selected_df: pd.DataFrame) -> pd.DataFrame:
    """Compute pairwise similarities between offers within a subcategory.

    Computes name, partial name, full name, image, description, and full
    description similarities for all offer pairs.

    Note:
        Pairs are first filtered by partial name similarity threshold before
        the more expensive description similarity is computed.

    Args:
        selected_df: DataFrame containing offers for a single subcategory with
            columns for offer ID, name, description, and image embedding.

    Returns:
        DataFrame of offer pairs that pass the partial name similarity threshold,
        with columns for each similarity metric as well as the raw offer names
        and descriptions for both items in the pair.
    """
    # 1. Compute name similarity
    preprocessed_names = selected_df[OFFER_NAME_COL].pipe(offer_name_preprocessing)
    name_similarity = rapidfuzz.process.cdist(
        preprocessed_names,
        preprocessed_names,
        processor=rapidfuzz.utils.default_process,
        scorer=fuzz.ratio,
        workers=-1,
        dtype=np.uint8,
    ).reshape(-1)
    logger.success("Name similarity computed")

    partial_name_similarity = rapidfuzz.process.cdist(
        preprocessed_names,
        preprocessed_names,
        processor=rapidfuzz.utils.default_process,
        scorer=fuzz.partial_ratio,
        workers=-1,
        score_cutoff=PARTIAL_NAME_SIMILARITY_THRESHOLD,
        dtype=np.uint8,
    ).reshape(-1)
    logger.success("Partial name similarity computed")

    preprocessed_full_names = selected_df[OFFER_NAME_COL].pipe(full_name_preprocessing)
    full_name_similarity = rapidfuzz.process.cdist(
        preprocessed_full_names,
        preprocessed_full_names,
        processor=rapidfuzz.utils.default_process,
        scorer=fuzz.ratio,
        workers=-1,
        dtype=np.uint8,
    ).reshape(-1)

    # 2. Compute image similarity
    _zero_image = np.zeros_like(selected_df[IMAGE_EMBEDDING_COL].dropna().iloc[0])
    image_embeddings = np.array(
        selected_df[IMAGE_EMBEDDING_COL]
        .apply(lambda x: _zero_image if x is None else x)
        .to_list()
    )  # Dim is (num_offers, embedding_size)
    image_scores = (image_embeddings @ image_embeddings.T).reshape(-1)
    logger.success("Image similarity computed")

    # 3. Create similarities_df with columns
    #   OFFER_ID_1, OFFER_ID_2, NAME_SIMILARITY, PARTIAL_NAME_SIMILARITY,
    #   FULL_NAME_SIMILARITY, IMAGE_SIMILARITY
    similarities_df = (
        selected_df.loc[:, [OFFER_ID_COL]]
        .merge(selected_df.loc[:, [OFFER_ID_COL]], how="cross", suffixes=("_1", "_2"))
        .assign(
            **{
                NAME_SIMILARITY_COL: name_similarity,
                PARTIAL_NAME_SIMILARITY_COL: partial_name_similarity,
                FULL_NAME_SIMILARITY_COL: full_name_similarity,
                IMAGE_SIMILARITY_COL: image_scores,
            }
        )
    )

    # 4. Filter out pairs with low partial name similarity
    filtered_df = similarities_df.loc[
        lambda df: df[f"{OFFER_ID_COL}_1"] > df[f"{OFFER_ID_COL}_2"]
    ].loc[
        lambda df: df[PARTIAL_NAME_SIMILARITY_COL] >= PARTIAL_NAME_SIMILARITY_THRESHOLD
    ]

    # 5. Compute description similarity using rapidfuzz on the filtered pairs only
    #    to save time
    offer_id_to_description = selected_df.set_index(OFFER_ID_COL)[
        OFFER_DESCRIPTION_COL
    ].to_dict()
    offer_id_to_name = selected_df.set_index(OFFER_ID_COL)[OFFER_NAME_COL].to_dict()

    logger.info(f"Computing description similarity for {len(filtered_df)} pairs...")
    preprocessed_descriptions_1 = (
        filtered_df[f"{OFFER_ID_COL}_1"]
        .map(offer_id_to_description)
        .pipe(description_preprocessing, min_length=MIN_DESCRIPTION_LENGTH)
    )
    preprocessed_descriptions_2 = (
        filtered_df[f"{OFFER_ID_COL}_2"]
        .map(offer_id_to_description)
        .pipe(description_preprocessing, min_length=MIN_DESCRIPTION_LENGTH)
    )
    description_similarity = rapidfuzz.process.cpdist(
        preprocessed_descriptions_1,
        preprocessed_descriptions_2,
        scorer=fuzz.partial_ratio,
        processor=rapidfuzz.utils.default_process,
        workers=-1,
        dtype=np.uint8,
    )
    logger.success("Description similarity computed")

    logger.info(
        f"Computing full description similarity for {len(filtered_df)} pairs..."
    )
    full_description_similarity = rapidfuzz.process.cpdist(
        preprocessed_descriptions_1,
        preprocessed_descriptions_2,
        scorer=fuzz.ratio,
        processor=rapidfuzz.utils.default_process,
        workers=-1,
        dtype=np.uint8,
    )
    logger.success("Full description similarity computed")

    # 6. Add description similarity to filtered df and return
    return filtered_df.assign(
        **{
            DESCRIPTION_SIMILARITY_COL: description_similarity,
            FULL_DESCRIPTION_SIMILARITY_COL: full_description_similarity,
            f"{OFFER_NAME_COL}_1": filtered_df[f"{OFFER_ID_COL}_1"].map(
                offer_id_to_name
            ),
            f"{OFFER_NAME_COL}_2": filtered_df[f"{OFFER_ID_COL}_2"].map(
                offer_id_to_name
            ),
            f"{OFFER_DESCRIPTION_COL}_1": filtered_df[f"{OFFER_ID_COL}_1"].map(
                offer_id_to_description
            ),
            f"{OFFER_DESCRIPTION_COL}_2": filtered_df[f"{OFFER_ID_COL}_2"].map(
                offer_id_to_description
            ),
        }
    ).reset_index(drop=True)


@app.command()
def main(
    offer_event_with_embeddings_filepath: str = typer.Option(),
    output_filepath: str = typer.Option(),
) -> None:
    # 1. Load Data
    offers_df = pd.read_parquet(offer_event_with_embeddings_filepath).sort_values(
        by=OFFER_NAME_COL
    )

    # 2. Compute similarities for each category separately
    all_similarities_dfs = []
    for subcategory in offers_df[OFFER_SUBCATEGORY_ID_COL].dropna().unique():
        offers_per_subcategory_df = (
            offers_df.loc[lambda df, s=subcategory: df[OFFER_SUBCATEGORY_ID_COL] == s]
            .loc[
                :,
                [
                    OFFER_ID_COL,
                    OFFER_NAME_COL,
                    OFFER_DESCRIPTION_COL,
                    IMAGE_EMBEDDING_COL,
                ],
            ]
            .reset_index(drop=True)
        )

        logger.info(
            f"Computing similarities for subcategory {subcategory} with "
            f"{len(offers_per_subcategory_df)} offers..."
        )
        all_similarities_dfs.append(compute_similarities(offers_per_subcategory_df))
        logger.success(f"Similarities computed for subcategory {subcategory}")

    # 3. Concat and save results
    pd.concat(all_similarities_dfs, ignore_index=True).to_parquet(
        output_filepath, index=False
    )
    logger.success(f"All similarities computed and saved to {output_filepath}")


if __name__ == "__main__":
    app()
