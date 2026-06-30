import typer

from handler import BigqueryDBTHandler, MetabaseDBTHandler
from utils import ENVIRONMENT_SHORT_NAME, METABASE_DEFAULT_DATABASE

app = typer.Typer()


@app.command()
def export_models(
    airflow_bucket_name: str = typer.Option(
        ...,
        help="Airflow Bucket where the manifest is present",
    ),
    airflow_bucket_manifest_path: str = typer.Option(
        "data/target/manifest.json",
        help="Path to the manifest file in the Airflow Bucket",
    ),
    local_manifest_path: str = typer.Option(
        "manifest.json",
        help="Path to the local manifest file",
    ),
):
    handler = MetabaseDBTHandler(
        airflow_bucket_name, airflow_bucket_manifest_path, local_manifest_path
    )
    handler.export_model(
        metabase_database=METABASE_DEFAULT_DATABASE,
        schema_filters=[f"analytics_{ENVIRONMENT_SHORT_NAME}"],
        model_names=["mrt_*"],
    )


@app.command()
def export_exposures(
    airflow_bucket_name: str = typer.Option(
        ...,
        help="Airflow Bucket where the manifest is present",
    ),
    exposure_dataset_name: str = typer.Option(
        ...,
        help="Dataset name for the exposure table",
    ),
    exposure_table_name: str = typer.Option(
        ...,
        help="Table name for the exposure table",
    ),
    airflow_bucket_manifest_path: str = typer.Option(
        "data/target/manifest.json",
        help="Path to the manifest file in the Airflow Bucket",
    ),
    airflow_bucket_base_folder: str = typer.Option(
        "data/exposures/",
        help="Base folder in the Airflow Bucket to save the exposures",
    ),
    local_manifest_path: str = typer.Option(
        "manifest.json",
        help="Path to the local manifest file",
    ),
):
    handler = MetabaseDBTHandler(
        airflow_bucket_name, airflow_bucket_manifest_path, local_manifest_path
    )
    bigquery_handler = BigqueryDBTHandler()
    candidates_df = bigquery_handler.get_exposure_candidates(
        dataset_name=exposure_dataset_name, table_name=exposure_table_name
    )

    collection_filters = bigquery_handler.candidate_collection_filters(candidates_df)
    classification = bigquery_handler.build_classification_lookup(candidates_df)

    handler.export_exposures(collection_filters=collection_filters, output_path=".")
    handler.inject_exposure_meta(
        exposures_path="exposures.yml",
        classification_by_asset=classification,
    )
    handler.push_exposures_to_bucket(
        airflow_bucket_name=airflow_bucket_name,
        airflow_bucket_path=f"{airflow_bucket_base_folder}exposures.yml",
        exposure_local_path="exposures.yml",
    )


if __name__ == "__main__":
    app()
