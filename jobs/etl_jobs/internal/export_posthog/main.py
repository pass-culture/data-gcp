from utils import access_secret_data, PROJECT_NAME, ENV_SHORT_NAME
import typer
from bq import bq_to_events
from event import EventExporter
import time

posthog_api_key = access_secret_data(PROJECT_NAME, f"posthog_api_key_{ENV_SHORT_NAME}")
posthog_host = access_secret_data(PROJECT_NAME, f"posthog_host_{ENV_SHORT_NAME}")
posthog_personal_api_key = access_secret_data(
    PROJECT_NAME, f"posthog_personal_api_key_{ENV_SHORT_NAME}"
)


BATCH_SIZE = 500
TIME = 15


def run(
    source_gs_path: str = typer.Option(
        ...,
        help="source_gs_path",
    ),
):
    print(f"Download all rows......")
    events = bq_to_events(source_gs_path)
    ph = EventExporter(
        posthog_api_key=posthog_api_key,
        posthog_host=posthog_host,
        posthog_personal_api_key=posthog_personal_api_key,
    )
    print(f"Will process {len(events)} events...")
    event_idx = 0
    for event_idx, event in enumerate(events, 1):
        ph.event_to_posthog(event)
        if event_idx % BATCH_SIZE == 0:
            print(f"Processed {event_idx} events. Pausing for {TIME} second...")
            time.sleep(TIME)
    print(f"Processed {event_idx} events.... Wait for finish")


if __name__ == "__main__":
    typer.run(run)
