import multiprocessing as mp
import os
import secrets
from functools import partial
from typing import Any, Dict, Optional

import numpy as np
import polars as pl
import pyarrow.dataset as ds
import typer
from loguru import logger

from tools.dimension_reduction import (
    pca_reduce_embedding_dimension,
    pumap_reduce_embedding_dimension,
    umap_reduce_embedding_dimension,
)
from tools.utils import (
    convert_arr_emb_to_str,
    convert_str_emb_to_float,
    export_polars_to_bq,
    load_config_file,
)


def reduce_transformation(
    X: np.ndarray,
    target_dimension: int,
    emb_col: str,
    method: str = "PUMAP",
    max_dimension: int = 32,
) -> np.ndarray:
    """
    Reduces X dimension according to method among ["PCA", "UMAP", "PUMAP"]
    If chosen method is "PCA":
        - only PCA reduction is preformed
    If chosen method is "UMAP" or "PUMAP":
        If X's second dimension is bigger than max_dimension:
            - we first reduce X with PCA so its second dimesnion is equal to the max_dimension
            - then we perform the UMAP or the PUMAP.
        If X's second dimension is smaller than max_dimension:
            - we directly reduce with the UMAP or the PUMAP.

    Args:
        X (np.ndarray): The numpy array to reduce
        target_dimension (int): number of components to keep during reduction
        emb_col(str): name of the column which X is extracted from in the original dataframe
        method (str): the reduction method to use. Options are ["PCA", "UMAP", "PUMAP"]
        max_dimension (int): dimension threshold for X's second dimension to decide whether to perform a PCA before the UMAP/PUMAP or not

    Returns:
        np.ndarray: the reduced array
    """
    seed = secrets.randbelow(1000)
    logger.info(f"Seed for PCA reduction set to {seed}")
    if method in ("UMAP", "PUMAP"):
        logger.info(f"Reducing first with PCA {emb_col}...")
        current_dimension = X.shape[1]
        if current_dimension > max_dimension:
            X = pca_reduce_embedding_dimension(X, dimension=max_dimension, seed=seed)
        else:
            logger.info(
                f"Current dimension {current_dimension} lower than {max_dimension}"
            )

        if method == "UMAP":
            logger.info(f"Reducing with UMAP {emb_col}...")
            X = umap_reduce_embedding_dimension(X, target_dimension)
        elif method == "PUMAP":
            logger.info(f"Reducing with PUMAP {emb_col}...")
            X = pumap_reduce_embedding_dimension(
                X, target_dimension, batch_size=2048, train_frac=0.1
            )
    elif method == "PCA":
        logger.info(f"Reducing with PCA {emb_col}...")
        X = pca_reduce_embedding_dimension(X, dimension=target_dimension, seed=seed)
    else:
        raise Exception("Method not found.")
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
    """Process reduction in batches and with optional caching"""
    # Calculate total rows to process
    total_rows = df.height
    logger.info(f"Processing {total_rows} rows in batches of {batch_size}")

    # Process in batches
    result_dfs = []
    for i in range(0, total_rows, batch_size):
        batch_end = min(i + batch_size, total_rows)
        logger.info(f"Processing batch {i}-{batch_end} of {total_rows}")
        batch_df = df.slice(i, batch_end - i)

        concat_X = []
        for emb_col in embedding_columns:
            logger.info(f"Converting serialized embeddings... {emb_col}...")

            # Try to load from cache if available
            cache_loaded = False
            if cache_dir:
                cache_file = os.path.join(
                    cache_dir, f"{emb_col}_batch_{i}_{batch_end}.npy"
                )
                if os.path.exists(cache_file):
                    try:
                        X = np.load(cache_file)
                        cache_loaded = True
                        logger.info(f"Loaded {emb_col} from cache")
                    except Exception as e:
                        logger.warning(f"Failed to load cache: {e}")

            if not cache_loaded:
                X = np.array(convert_str_emb_to_float(batch_df[emb_col]))
                X = reduce_transformation(
                    X,
                    target_dimension,
                    emb_col,
                    method=method,
                    max_dimension=max_dimension,
                )

                # Save to cache if enabled
                if cache_dir:
                    os.makedirs(cache_dir, exist_ok=True)
                    cache_file = os.path.join(
                        cache_dir, f"{emb_col}_batch_{i}_{batch_end}.npy"
                    )
                    np.save(cache_file, X)

            concat_X.append(X)
            batch_df = batch_df.with_columns(
                pl.Series(name=emb_col, values=convert_arr_emb_to_str(X))
            )
            logger.info(f"Done for {emb_col}...")

        logger.info("Reduce whole columns")
        X = reduce_transformation(
            np.concatenate(concat_X, axis=1),
            target_dimension,
            emb_col=target_name,
            method=method,
            max_dimension=max_dimension,
        )
        batch_df = batch_df.with_columns(
            pl.Series(name=target_name, values=convert_arr_emb_to_str(X))
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
