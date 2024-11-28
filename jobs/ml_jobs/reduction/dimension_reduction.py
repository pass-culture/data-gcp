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
    X, target_dimension, emb_col, method="PUMAP", max_dimension=32, seed=0
):
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
    seed=0,
):
    concat_X = []
    for emb_col in embedding_columns:
        logger.info(f"Convert serialized embeddings... {emb_col}...")
        X = np.array(convert_str_emb_to_float(df[emb_col]))
        X = reduce_transformation(
            X,
            target_dimension,
            emb_col,
            method=method,
            max_dimension=max_dimension,
            seed=seed,
        )
        concat_X.append(X)
        logger.info(f"Process done {emb_col}...")
        df = df.with_columns(pl.Series(name=emb_col, values=convert_arr_emb_to_str(X)))
        logger.info(f"Done for {emb_col}...")

    logger.info("Reduce whole columns")
    X = reduce_transformation(
        np.concatenate(concat_X, axis=1),
        target_dimension,
        emb_col=target_name,
        method=method,
        max_dimension=max_dimension,
        seed=seed,
    )
    df = df.with_columns(pl.Series(name=target_name, values=convert_arr_emb_to_str(X)))

    return df.with_columns(reduction_method=pl.lit(method))


def plan(
    source_gs_path,
    embedding_columns,
    output_dataset_name,
    output_table_prefix,
    target_dimension,
    target_name,
    method,
    max_dimension,
    seed,
):
    dataset = ds.dataset(source_gs_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    export_cols = ["item_id"] + embedding_columns
    output_table_name = f"{output_table_prefix}_reduced_{target_dimension}"
    logger.info(f"Reducing Table... {output_table_name}")
    ldf = export_reduction_table(
        ldf.select(export_cols).collect(),
        target_dimension=target_dimension,
        target_name=target_name,
        embedding_columns=embedding_columns,
        method=method,
        max_dimension=max_dimension,
        seed=seed,
    )
    export_polars_to_bq(
        data=ldf.select(
            ["item_id", "reduction_method"] + embedding_columns + [target_name]
        ),
        dataset=output_dataset_name,
        output_table=output_table_name,
    )
    logger.info(f"Done Table... {output_table_name}")


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
    seed: int = typer.Option(
        0,
        help="Seed to fix random state of PCA space reduction",
    ),
) -> None:
    """ """
    config_json = load_config_file(config_file_name, job_type="reduction")[
        "reduction_configs"
    ][reduction_config]
    ###############
    # Load preprocessed data
    for params in config_json:
        target_dimension = params["target_dimensions"]
        target_name = params["target_name"]
        method = params["method"]
        embedding_columns = params["embedding_columns"]
        pre_reduction_dim = params.get("pca_pre_reduction_dimension", 32)
        plan(
            source_gs_path,
            embedding_columns,
            output_dataset_name=output_dataset_name,
            output_table_prefix=output_prefix_table_name,
            target_dimension=target_dimension,
            target_name=target_name,
            method=method,
            max_dimension=pre_reduction_dim,
            seed=seed,
        )


if __name__ == "__main__":
    typer.run(dimension_reduction)
