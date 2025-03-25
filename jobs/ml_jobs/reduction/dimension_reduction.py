import multiprocessing as mp
import os
import secrets
from functools import partial
from typing import Any, Dict, Optional, Tuple

import numpy as np
import polars as pl
import pyarrow.dataset as ds
import typer
from loguru import logger
from sklearn.decomposition import PCA

from tools.dimension_reduction import (
    pumap_reduce_embedding_dimension,
    umap_reduce_embedding_dimension,
)
from tools.utils import (
    convert_arr_emb_to_str,
    convert_str_emb_to_float,
    export_polars_to_bq,
    load_config_file,
)


def fit_reduction_model(
    X: np.ndarray,
    target_dimension: int,
    emb_col: str,
    method: str = "PUMAP",
    max_dimension: int = 32,
) -> Tuple[Any, np.ndarray]:
    """
    Fits a reduction model based on the entire dataset X

    Args:
        X: Input data array
        target_dimension: Target dimension for reduction
        emb_col: Name of embedding column for logging
        method: Reduction method (PCA, UMAP, PUMAP)
        max_dimension: Maximum intermediate dimension

    Returns:
        Tuple of (transformed_data, models)
        - models: Dictionary of fitted model objects
    """
    seed = secrets.randbelow(1000)
    logger.info(f"Seed for reduction set to {seed}")
    models = {}

    # For UMAP or PUMAP, we might need a PCA first
    if method in ("UMAP", "PUMAP"):
        logger.info(f"Fitting PCA model for {emb_col}...")
        current_dimension = X.shape[1]
        if current_dimension > max_dimension:
            # Fit and apply PCA
            pca_model = PCA(n_components=max_dimension, random_state=seed)
            X = pca_model.fit_transform(X)
            models["pca"] = pca_model
            logger.info(
                f"Fitted PCA model: explained variance ratio sum = {np.sum(pca_model.explained_variance_ratio_):.3f}"
            )
        else:
            logger.info(
                f"Current dimension {current_dimension} lower than {max_dimension}, skipping PCA"
            )

        # Fit UMAP/PUMAP
        if method == "UMAP":
            logger.info(f"Fitting UMAP model for {emb_col}...")
            # Get the UMAP model and transformed data
            X, umap_model = umap_reduce_embedding_dimension(
                X, target_dimension, return_model=True
            )
            models["umap"] = umap_model
        elif method == "PUMAP":
            logger.info(f"Fitting PUMAP model for {emb_col}...")
            # Get the PUMAP model and transformed data
            X, pumap_model = pumap_reduce_embedding_dimension(
                X, target_dimension, batch_size=2048, train_frac=0.1, return_model=True
            )
            models["pumap"] = pumap_model
    elif method == "PCA":
        logger.info(f"Fitting PCA model for {emb_col}...")
        # Fit and apply PCA
        pca_model = PCA(n_components=target_dimension, random_state=seed)
        X = pca_model.fit_transform(X)
        models["pca"] = pca_model
        logger.info(
            f"Fitted PCA model: explained variance ratio sum = {np.sum(pca_model.explained_variance_ratio_):.3f}"
        )
    else:
        raise Exception(f"Method {method} not found.")

    return models, X


def apply_reduction_model(
    X: np.ndarray, models: Dict[str, Any], method: str, emb_col: str
) -> np.ndarray:
    """
    Applies pre-fitted reduction models to transform data

    Args:
        X: Input data array
        models: Dictionary of fitted model objects
        method: Reduction method (PCA, UMAP, PUMAP)
        emb_col: Name of embedding column for logging

    Returns:
        Transformed data array
    """
    logger.info(f"Applying pre-fitted models to transform {emb_col}...")

    if method in ("UMAP", "PUMAP"):
        # Apply PCA first if it was fitted
        if "pca" in models:
            logger.info(f"Applying fitted PCA model to {emb_col}...")
            X = models["pca"].transform(X)

        # Apply UMAP/PUMAP
        if method == "UMAP":
            logger.info(f"Applying fitted UMAP model to {emb_col}...")
            X = models["umap"].transform(X)
        elif method == "PUMAP":
            logger.info(f"Applying fitted PUMAP model to {emb_col}...")
            X = models["pumap"].transform(X)
    elif method == "PCA":
        # Apply PCA
        logger.info(f"Applying fitted PCA model to {emb_col}...")
        X = models["pca"].transform(X)
    else:
        raise Exception(f"Method {method} not found.")

    return X


def export_reduction_table(
    df,
    target_dimension,
    target_name,
    embedding_columns,
    method="PUMAP",
    max_dimension=32,
    batch_size=10000,
    cache_dir=None,
):
    """Process reduction with two-phase approach: fit on all data, transform in batches"""
    # First phase: Fit models on all data
    reduction_models = {}
    all_transformed = {}

    for emb_col in embedding_columns:
        logger.info(f"Loading all data for {emb_col} to fit models...")
        X = np.array(convert_str_emb_to_float(df[emb_col]))

        # Fit models on all data at once
        models, transformed = fit_reduction_model(
            X,
            target_dimension,
            emb_col,
            method=method,
            max_dimension=max_dimension,
        )
        reduction_models[emb_col] = models

        # Store full transformed data for later use
        all_transformed[emb_col] = transformed

    # For the combined columns reduction
    logger.info(f"Fitting models for combined embeddings ({target_name})...")
    concat_X = np.concatenate(
        [all_transformed[col] for col in embedding_columns], axis=1
    )
    combined_models, combined_transformed = fit_reduction_model(
        concat_X,
        target_dimension,
        emb_col=target_name,
        method=method,
        max_dimension=max_dimension,
    )
    reduction_models[target_name] = combined_models

    # Second phase: Apply transformations in batches for I/O efficiency
    total_rows = df.height
    result_dfs = []

    for i in range(0, total_rows, batch_size):
        batch_end = min(i + batch_size, total_rows)
        logger.info(f"Processing batch {i}-{batch_end} of {total_rows}")
        batch_df = df.slice(i, batch_end - i)

        # Apply each embedding column transformation
        for emb_col in embedding_columns:
            transformed = all_transformed[emb_col][i:batch_end]
            batch_df = batch_df.with_columns(
                pl.Series(name=emb_col, values=convert_arr_emb_to_str(transformed))
            )

        # Apply combined transformation
        combined_transformed_batch = combined_transformed[i:batch_end]
        batch_df = batch_df.with_columns(
            pl.Series(
                name=target_name,
                values=convert_arr_emb_to_str(combined_transformed_batch),
            )
        )
        batch_df = batch_df.with_columns(reduction_method=pl.lit(method))
        result_dfs.append(batch_df)

    # Combine all batch results
    return pl.concat(result_dfs)


def process_config(
    params: Dict[str, Any],
    source_gs_path: str,
    output_dataset_name: str,
    output_table_prefix: str,
    cache_dir: Optional[str] = None,
    batch_size: int = 10000,
):
    """Process a single reduction configuration"""
    target_dimension = params["target_dimensions"]
    target_name = params["target_name"]
    method = params["method"]
    embedding_columns = params["embedding_columns"]
    pre_reduction_dim = params.get("pca_pre_reduction_dimension", 32)

    output_table_name = f"{output_table_prefix}_reduced_{target_dimension}"
    logger.info(f"Reducing Table... {output_table_name}")

    dataset = ds.dataset(source_gs_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    export_cols = ["item_id"] + embedding_columns

    df = ldf.select(export_cols).collect()
    df = export_reduction_table(
        df,
        target_dimension=target_dimension,
        target_name=target_name,
        embedding_columns=embedding_columns,
        method=method,
        max_dimension=pre_reduction_dim,
        batch_size=batch_size,
        cache_dir=cache_dir,
    )

    export_polars_to_bq(
        data=df.select(
            ["item_id", "reduction_method"] + embedding_columns + [target_name]
        ),
        dataset=output_dataset_name,
        output_table=output_table_name,
    )
    logger.info(f"Done Table... {output_table_name}")
    return output_table_name


def dimension_reduction(
    config_file_name: str = typer.Option(
        "default-config-offer", help="Config file name"
    ),
    source_gs_path: str = typer.Option(
        ...,
        help="Name of the dataframe we want to reduce",
    ),
    output_dataset_name: str = typer.Option(
        ...,
        help="Name of the output dataset",
    ),
    output_prefix_table_name: str = typer.Option(
        ...,
        help="Name of the output prefix table",
    ),
    reduction_config: str = typer.Option(
        "default",
        help="String for the configuration plan to execute",
    ),
    parallel: bool = typer.Option(
        True,
        help="Process configurations in parallel",
    ),
    num_workers: int = typer.Option(
        None,
        help="Number of worker processes (defaults to CPU count if not specified)",
    ),
    batch_size: int = typer.Option(
        10000,
        help="Number of rows to process in each batch",
    ),
    use_cache: bool = typer.Option(
        True,
        help="Cache intermediate results to disk",
    ),
    cache_dir: str = typer.Option(
        "/tmp/dim_reduction_cache",
        help="Directory to store cached intermediate results",
    ),
) -> None:
    """Run dimension reduction with performance optimizations"""
    config_json = load_config_file(config_file_name, job_type="reduction")[
        "reduction_configs"
    ][reduction_config]

    if not num_workers and parallel:
        num_workers = mp.cpu_count() - 1 or 1
        logger.info(f"Using {num_workers} worker processes")

    cache_directory = cache_dir if use_cache else None
    if use_cache:
        os.makedirs(cache_directory, exist_ok=True)
        logger.info(f"Using cache directory: {cache_directory}")

    if parallel and len(config_json) > 1:
        logger.info(f"Processing {len(config_json)} configurations in parallel")
        process_func = partial(
            process_config,
            source_gs_path=source_gs_path,
            output_dataset_name=output_dataset_name,
            output_table_prefix=output_prefix_table_name,
            cache_dir=cache_directory,
            batch_size=batch_size,
        )

        with mp.Pool(processes=num_workers) as pool:
            processed_tables = pool.map(process_func, config_json)
        logger.info(f"Completed processing tables: {processed_tables}")
    else:
        # Sequential processing
        for params in config_json:
            process_config(
                params,
                source_gs_path=source_gs_path,
                output_dataset_name=output_dataset_name,
                output_table_prefix=output_prefix_table_name,
                cache_dir=cache_directory,
                batch_size=batch_size,
            )


if __name__ == "__main__":
    typer.run(dimension_reduction)
