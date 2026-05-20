import numpy as np
import pandas as pd
import rapidfuzz
import typer
from loguru import logger
from rapidfuzz import fuzz

from src.constants import (
    DESCRIPTION_SIMILARITY_COL,
    IMAGE_EMBEDDING_COLUMN,
    IMAGE_SIMILARITY_COL,
    NAME_SIMILARITY_COL,
    OFFER_DESCRIPTION_COL,
    OFFER_ID_COLUMN,
    OFFER_NAME_COL,
    OFFER_SUBCATEGORY_ID_COL,
    PARTIAL_NAME_SIMILARITY_COL,
)

FUZZ_THRESHOLD = 70
MIN_DESCRIPTION_LENGTH = 30


app = typer.Typer()


def description_preprocessing(series: pd.Series) -> pd.Series:
    return (
        series.str.lower()
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .where(lambda s: s.str.len() >= MIN_DESCRIPTION_LENGTH, other=pd.NA)
    )


def offer_name_preprocessing(series: pd.Series) -> pd.Series:
    return (
        series.str.lower()
        .str.replace(
            r"^(?:concert de poche|apero-concert|concerts?)\W*", "", regex=True
        )
        .str.split(r"/|\+|&|•|\||:| - | à | en concert | x | et | au | avec ")
        .str[0]
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .replace("", pd.NA)
    )


def compute_similarities(selected_df: pd.DataFrame) -> pd.DataFrame:
    # 1. Compute name similarity
    name_similarity = rapidfuzz.process.cdist(
        selected_df[OFFER_NAME_COL].pipe(offer_name_preprocessing),
        selected_df[OFFER_NAME_COL].pipe(offer_name_preprocessing),
        processor=rapidfuzz.utils.default_process,
        scorer=fuzz.ratio,
        workers=-1,
        dtype=np.uint8,
    ).reshape(-1)
    logger.success("Name similarity computed")

    partial_name_similarity = rapidfuzz.process.cdist(
        selected_df[OFFER_NAME_COL].pipe(offer_name_preprocessing),
        selected_df[OFFER_NAME_COL].pipe(offer_name_preprocessing),
        processor=rapidfuzz.utils.default_process,
        scorer=fuzz.partial_ratio,
        workers=-1,
        score_cutoff=FUZZ_THRESHOLD,
        dtype=np.uint8,
    ).reshape(-1)
    logger.success("Partial name similarity computed")

    # 2. Compute image similarity
    _zero_image = np.zeros_like(selected_df[IMAGE_EMBEDDING_COLUMN].dropna().iloc[0])
    image_embedding = np.array(
        selected_df[IMAGE_EMBEDDING_COLUMN]
        .apply(lambda x: _zero_image if x is None else x)
        .to_list()
    )
    image_scores = (image_embedding @ image_embedding.T).reshape(-1)
    logger.success("Image similarity computed")

    # 3. Create similarities_df
    similarities_df = (
        selected_df.loc[:, [OFFER_ID_COLUMN]]
        .merge(
            selected_df.loc[:, [OFFER_ID_COLUMN]], how="cross", suffixes=("_1", "_2")
        )
        .assign(
            **{
                NAME_SIMILARITY_COL: name_similarity,
                PARTIAL_NAME_SIMILARITY_COL: partial_name_similarity,
                IMAGE_SIMILARITY_COL: image_scores,
            }
        )
    )

    # 4. Filter out pairs with low partial name similarity
    filtered_df = similarities_df.loc[
        lambda df: df[f"{OFFER_ID_COLUMN}_1"] > df[f"{OFFER_ID_COLUMN}_2"]
    ].loc[lambda df: df[PARTIAL_NAME_SIMILARITY_COL] > FUZZ_THRESHOLD]

    # 5. Compute description similarity using rapidfuzz on the filtered pairs only
    #    to save time
    offer_id_to_description = selected_df.set_index(OFFER_ID_COLUMN)[
        OFFER_DESCRIPTION_COL
    ].to_dict()
    offer_id_to_name = selected_df.set_index(OFFER_ID_COLUMN)[OFFER_NAME_COL].to_dict()
    logger.info(f"Computing description similarity for {len(filtered_df)} pairs...")
    description_similarity = rapidfuzz.process.cpdist(
        filtered_df[f"{OFFER_ID_COLUMN}_1"]
        .map(offer_id_to_description)
        .pipe(description_preprocessing),
        filtered_df[f"{OFFER_ID_COLUMN}_2"]
        .map(offer_id_to_description)
        .pipe(description_preprocessing),
        scorer=fuzz.partial_ratio,
        processor=rapidfuzz.utils.default_process,
        workers=-1,
        dtype=np.uint8,
    )
    logger.success("Description similarity computed")

    # 6. Add description similarity to filtered df and return
    return filtered_df.assign(
        **{
            DESCRIPTION_SIMILARITY_COL: description_similarity,
            f"{OFFER_NAME_COL}_1": filtered_df[f"{OFFER_ID_COLUMN}_1"].map(
                offer_id_to_name
            ),
            f"{OFFER_NAME_COL}_2": filtered_df[f"{OFFER_ID_COLUMN}_2"].map(
                offer_id_to_name
            ),
            f"{OFFER_DESCRIPTION_COL}_1": filtered_df[f"{OFFER_ID_COLUMN}_1"].map(
                offer_id_to_description
            ),
            f"{OFFER_DESCRIPTION_COL}_2": filtered_df[f"{OFFER_ID_COLUMN}_2"].map(
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
                    OFFER_ID_COLUMN,
                    OFFER_NAME_COL,
                    OFFER_DESCRIPTION_COL,
                    IMAGE_EMBEDDING_COLUMN,
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
