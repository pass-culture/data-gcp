import numpy as np
import typing as t
from datetime import datetime
import uuid
from utils import PostHogEvent, ENV_SHORT_NAME


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
    for x, y in zip(row[1:], list(df.columns)):
        if isinstance(x, np.ndarray):
            _dict[y] = export_list_metadata(np.ndarray.flatten(x))
        else:
            _dict[y] = x
    return _dict


def bq_to_events(df) -> t.List[PostHogEvent]:
    rows = []
    for row in df.itertuples():
        _dict = row_to_dict(row, df)
        rows.append(format_event(_dict))
    return rows


def format_event(event: dict) -> PostHogEvent:
    user_pseudo_id = event["user_pseudo_id"]
    origin = event["origin"]
    event_time = datetime.utcfromtimestamp(event["event_timestamp"] / 1e6)

    unique_event = (
        user_pseudo_id + str(event_time) + str(event["event_params"]["ga_session_id"])
    )
    event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, unique_event)
    event_params = event["event_params"]
    event_type = event["event_name"]

    user_params = {
        "user_id": event.get("user_id"),
        "platform": event.get("platform"),
    }

    properties = dict(**user_params, **event_params)

    return PostHogEvent(
        environment=ENV_SHORT_NAME,
        origin=origin,
        device_id=user_pseudo_id,
        event_type=event_type,
        properties=properties,
        timestamp=event_time,
        uuid=str(event_uuid),
    )
