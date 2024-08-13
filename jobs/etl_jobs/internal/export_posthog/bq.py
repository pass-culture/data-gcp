import uuid

import numpy as np
import polars as pl
import pyarrow.dataset as ds
from utils import ENV_SHORT_NAME, PostHogEvent

BATCH_SIZE = 100_000


def export_type(values):
    for k, v in values.items():
        if v is not None:
            return v


def export_list_metadata(list_metadata):
    _dict = {}
    for z in list_metadata:
        key = z["key"]
        value = z["value"]
        if isinstance(z, dict):
            value = export_type(value)
        _dict[key] = value
    return _dict


def row_to_dict(row, df):
    _dict = {}
    for x, y in zip(row[1:], list(df.columns), strict=False):
        if isinstance(x, np.ndarray):
            _dict[y] = export_list_metadata(np.ndarray.flatten(x))
        else:
            _dict[y] = x
    return _dict


def download_df(bucket_path):
    # download
    dataset = ds.dataset(bucket_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    return ldf.collect().to_pandas()


def bq_to_events(source_gs_path) -> list[PostHogEvent]:
    print("Download...")
    df = download_df(bucket_path=source_gs_path)
    print(f"Reformat... {df.shape[0]}")
    rows = []
    for event_idx, row in enumerate(df.itertuples()):
        _dict = row_to_dict(row, df)
        rows.append(format_event(_dict))
        if event_idx % BATCH_SIZE == 0:
            print(f"Processed {event_idx} events.")
    return rows


def format_event(event: dict) -> PostHogEvent:
    user_pseudo_id = event["user_pseudo_id"]
    event_time = event["event_timestamp"].to_pydatetime()

    unique_event = user_pseudo_id + str(event_time)

    event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, unique_event)
    event_params = {**event.get("extra_params", {})}
    event_type = event["event_name"]

    user_params = {
        **{
            "user_id": event.get("user_id"),
            "platform": event.get("platform"),
            "firebase_app_version": event.get("app_version"),
            "environment": ENV_SHORT_NAME,
            "import_origin": event["origin"],
        },
        **event.get("user_params", {}),
    }

    properties = {**user_params, **event_params}

    return PostHogEvent(
        device_id=user_pseudo_id,
        event_type=event_type,
        properties=properties,
        timestamp=event_time,
        uuid=str(event_uuid),
        user_properties=user_params,
    )
