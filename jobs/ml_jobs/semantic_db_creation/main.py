import typer
from build_lancedb_table import (
    build_lancedb_table,
    create_index,
    prune_old_versions,
    versioned_uri,
    write_latest_manifest,
)


def main(
    gcs_embedding_parquet_file: str = typer.Option(
        help="GCS Parquet file or folder path"
    ),
    lancedb_uri: str = typer.Option(
        help="LanceDB root URI. Each build is written to an immutable "
        "`<root>/versions/<version>/` dir and a `<root>/latest.json` manifest "
        "is published to point readers at it."
    ),
    lancedb_table: str = typer.Option(help="LanceDB table name"),
    batch_size: int = typer.Option(help="Batch size for streaming"),
    vector_column_name: str = typer.Option(
        help="Name of the vector column in the parquet file"
    ),
    version: str = typer.Option(
        help="Immutable version id for this build (e.g. the DAG run `ts_nodash`)."
    ),
    keep_versions: int = typer.Option(
        3, help="Number of past versions to retain under `<root>/versions/`."
    ),
):
    """Create a versioned LanceDB table and publish the `latest.json` manifest.

    The table is built into an immutable `<root>/versions/<version>/` directory;
    the manifest is written only after the build + indexing succeed, so readers
    atomically switch to a complete artifact and can roll back to a retained
    previous version.
    """

    version_db_uri = versioned_uri(lancedb_uri, version)

    table = build_lancedb_table(
        gcs_embedding_parquet_file=gcs_embedding_parquet_file,
        lancedb_uri=version_db_uri,
        lancedb_table=lancedb_table,
        batch_size=batch_size,
        vector_column_name=vector_column_name,
    )
    create_index(table)

    write_latest_manifest(
        root_uri=lancedb_uri,
        version=version,
        version_uri=version_db_uri,
        table_name=lancedb_table,
        row_count=table.count_rows(),
    )
    prune_old_versions(lancedb_uri, keep_versions)


if __name__ == "__main__":
    typer.run(main)
