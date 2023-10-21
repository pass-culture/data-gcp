from utils import access_secret_data, PROJECT_NAME, ENV_SHORT_NAME
import typer
import pyarrow.dataset as ds
import polars as pl
from bq import bq_to_events
from event import EventExporter

posthog_api_key = access_secret_data(PROJECT_NAME, f"posthog_api_key_{ENV_SHORT_NAME}")
posthog_host = access_secret_data(PROJECT_NAME, f"posthog_host_{ENV_SHORT_NAME}")
posthog_personal_api_key = access_secret_data(
    PROJECT_NAME, f"posthog_personal_api_key_{ENV_SHORT_NAME}"
)


def download_df(bucket_path):
    # download
    dataset = ds.dataset(bucket_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    return ldf.collect()


def run(
    source_gs_path: int = typer.Option(
        ...,
        help="source_gs_path",
    ),
):
    df = download_df(bucket_path=source_gs_path)
    events = bq_to_events(df)
    ph = EventExporter(
        posthog_api_key=posthog_api_key,
        posthog_host=posthog_host,
        posthog_personal_api_key=posthog_personal_api_key,
    )
    for e in events:
        ph.event_to_posthog(e)


if __name__ == "__main__":
    typer.run(run)
