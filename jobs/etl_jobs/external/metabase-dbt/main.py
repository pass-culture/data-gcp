import typer

from handler import BigqueryDBTHandler, MetabaseDBTHandler

app = typer.Typer()


@app.command()
def export_models(
    composer_bucket_name: str = typer.Option(
        ...,
        help="Composer Bucket where the manifest is present",
    ),
    composer_bucket_manifest_path: str = typer.Option(
        "data/target/manifest.json",
        help="Path to the manifest file in the Composer Bucket",
    ),
    local_manifest_path: str = typer.Option(
        "manifest.json",
        help="Path to the local manifest file",
    ),
):
    handler = MetabaseDBTHandler(
        composer_bucket_name, composer_bucket_manifest_path, local_manifest_path
    )
    handler.export_model(schema_filters=["analytics_prod"], model_names=["mrt_*"])


@app.command()
def export_exposures(
    composer_bucket_name: str = typer.Option(
        ...,
        help="Composer Bucket where the manifest is present",
    ),
    exposure_dataset_name: str = typer.Option(
        ...,
        help="Dataset name for the exposure table",
    ),
    exposure_table_name: str = typer.Option(
        ...,
        help="Table name for the exposure table",
    ),
    composer_bucket_manifest_path: str = typer.Option(
        "data/target/manifest.json",
        help="Path to the manifest file in the Composer Bucket",
    ),
    composer_bucket_base_folder: str = typer.Option(
        "data/exposures/",
        help="Base folder in the Composer Bucket to save the exposures",
    ),
    local_manifest_path: str = typer.Option(
        "manifest.json",
        help="Path to the local manifest file",
    ),
):
    handler = MetabaseDBTHandler(
        composer_bucket_name, composer_bucket_manifest_path, local_manifest_path
    )
    df = BigqueryDBTHandler().get_bq_metabase_internal_exposure(
        dataset_name=exposure_dataset_name, table_name=exposure_table_name, limit=10
    )

    collection_filters = df["metabase_collection_name"].unique().tolist()
    handler.export_exposures(collection_filters=collection_filters, output_path=".")
    handler.push_exposures_to_bucket(
        composer_bucket_name=composer_bucket_name,
        composer_bucket_path=f"{composer_bucket_base_folder}exposures.yml",
        exposure_local_path="exposures.yml",
    )


if __name__ == "__main__":
    app()
