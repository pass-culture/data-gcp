import time

import numpy as np
import pandas as pd
import rapidfuzz
import typer
from loguru import logger
from rapidfuzz import fuzz

from src.constants import (
    IMAGE_EMBEDDING_COLUMN,
    OFFER_DESCRIPTION_COL,
    OFFER_ID_COLUMN,
    OFFER_NAME_COL,
    OFFER_SUBCATEGORY_ID_COL,
)

THRESHOLD = 70


app = typer.Typer()


def description_preprocessing(series: pd.Series) -> pd.Series:
    return (
        series.str.lower()
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .where(lambda s: s.str.len() >= 30, other=pd.NA)
    )


def offer_name_preprocessing(series: pd.Series) -> pd.Series:
    return (
        series.str.lower()
        .str.split(
            r"/|\+|&|•|\||:| - | à | en concert | x | et | au | avec |concert |concert de poche|apero-concert"  # noqa: E501
        )
        .str[0]
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .replace("", pd.NA)
    )


def compute_similarities(selected_df: pd.DataFrame) -> pd.DataFrame:
    # 1. Compute name similarity
    t0 = time.time()
    name_similarity = rapidfuzz.process.cdist(
        selected_df[OFFER_NAME_COL].pipe(offer_name_preprocessing),
        selected_df[OFFER_NAME_COL].pipe(offer_name_preprocessing),
        processor=rapidfuzz.utils.default_process,
        scorer=fuzz.ratio,
        workers=-1,
        dtype=np.uint8,
    ).reshape(-1)
    t1 = time.time()
    logger.success(f"Name similarity computed in {t1 - t0:.2f} seconds")
    t1 = time.time()
    partial_name_similarity = rapidfuzz.process.cdist(
        selected_df[OFFER_NAME_COL].pipe(offer_name_preprocessing),
        selected_df[OFFER_NAME_COL].pipe(offer_name_preprocessing),
        processor=rapidfuzz.utils.default_process,
        scorer=fuzz.partial_ratio,
        workers=-1,
        score_cutoff=THRESHOLD,
        dtype=np.uint8,
    ).reshape(-1)
    t2 = time.time()
    logger.success(f"Partial name similarity computed in {t2 - t1:.2f} seconds")

    # 2. Compute image similarity
    _zero_image = np.zeros_like(selected_df[IMAGE_EMBEDDING_COLUMN].dropna().iloc[0])
    image_embedding = np.array(
        selected_df[IMAGE_EMBEDDING_COLUMN]
        .apply(lambda x: _zero_image if x is None else x)
        .to_list()
    )
    image_scores = (image_embedding @ image_embedding.T).reshape(-1)
    t3 = time.time()
    logger.success(f"Image similarity computed in {t3 - t2:.2f} seconds")

    # 4. Create temp df with all similarities
    temp_df = (
        selected_df.loc[:, [OFFER_ID_COLUMN]]
        .merge(
            selected_df.loc[:, [OFFER_ID_COLUMN]], how="cross", suffixes=("_1", "_2")
        )
        .assign(
            image_similarity=image_scores,
            name_similarity=name_similarity,
            partial_name_similarity=partial_name_similarity,
        )
    )
    t4 = time.time()
    logger.success(f"Temp df computed in {t4 - t3:.2f} seconds")

    # 5. Filter pairs with high partial name similarity
    filtered_df = temp_df.loc[
        lambda df: df[f"{OFFER_ID_COLUMN}_1"] > df[f"{OFFER_ID_COLUMN}_2"]
    ].loc[lambda df: df["partial_name_similarity"] > THRESHOLD]

    # 6. Compute description similarity using rapidfuzz
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
    logger.success(f"Description similarity computed in {time.time() - t4:.2f} seconds")

    # 7. Add description similarity to filtered df and return
    return filtered_df.assign(
        description_similarity=description_similarity,
        description_1=filtered_df[f"{OFFER_ID_COLUMN}_1"].map(offer_id_to_description),
        description_2=filtered_df[f"{OFFER_ID_COLUMN}_2"].map(offer_id_to_description),
        name_1=filtered_df[f"{OFFER_ID_COLUMN}_1"].map(offer_id_to_name),
        name_2=filtered_df[f"{OFFER_ID_COLUMN}_2"].map(offer_id_to_name),
    ).reset_index(drop=True)


def main(
    offer_event_with_embbedings_filepath: str = typer.Option(),
    output_filepath: str = typer.Option(),
) -> None:
    # 1. Load Data
    offers_df = pd.read_parquet(offer_event_with_embbedings_filepath).sort_values(
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

        similarties_per_category_df = compute_similarities(offers_per_subcategory_df)
        all_similarities_dfs.append(similarties_per_category_df)
        logger.success(
            f"Similarities computed for subcategory {subcategory} with "
            f"{len(offers_per_subcategory_df)} offers and "
            f"{len(similarties_per_category_df)} similar pairs"
        )

    # 3. Concat and save results
    pd.concat(all_similarities_dfs, ignore_index=True).to_parquet(
        output_filepath, index=False
    )


if __name__ == "__main__":
    main()
