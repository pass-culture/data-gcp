import typer

from handler import MetabaseDBTHandler


def run(
    composer_bucket_name: str = typer.Option(
        ...,
        help="Composer Bucket where the manifest is present",
    ),
):
    handler = MetabaseDBTHandler(composer_bucket_name, manifest_path="catalog.json")
    handler.export_model(["mrt_*"])


if __name__ == "__main__":
    typer.run(run)
