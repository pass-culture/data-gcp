"""Input parquets and loaders shared by the evaluation scripts.

The four file names below are intentionally left EMPTY: fill them in (or pass the
matching CLI option) with the parquet files of the event_linkage run you want to
evaluate, so an evaluation always scores an explicitly chosen set of artefacts.

Names are resolved relative to `event_linkage/data/`; absolute paths also work.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.constants import EVENT_ID_COL, OFFER_ID_COL

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

# Offers submitted to the linker, e.g. "00_event_offer_to_link.parquet".
OFFERS_PARQUET = ""
# Pairwise similarities, e.g. "02_similarities.parquet".
SIMILARITIES_PARQUET = ""
# Event series delta, e.g. "03_delta_event_series.parquet".
EVENTS_PARQUET = ""
# Event/offer link delta, e.g. "03_delta_event_series_offer_link.parquet".
LINKS_PARQUET = ""

OFFER_COLS = [OFFER_ID_COL, "offer_name", "offer_description", "offer_subcategory_id"]
EVENT_COLS = [EVENT_ID_COL, "event_name", "event_description"]


def resolve(name: str, label: str) -> Path:
    """Turn a configured parquet name into a path, failing loudly when it is unset."""
    if not name:
        raise ValueError(
            f"No parquet configured for '{label}'. Set it in evaluation/data.py "
            "or pass the matching CLI option."
        )
    path = Path(name)
    if not path.is_absolute():
        path = DATA_DIR / path
    if not path.exists():
        raise FileNotFoundError(f"{label}: {path} does not exist.")
    return path


def load_links(name: str) -> pd.DataFrame:
    """Net linkage state (event_id, offer_id) from the add/remove delta changelog."""
    links = pd.read_parquet(resolve(name, "links"))
    added = links.loc[links.action == "add", [EVENT_ID_COL, OFFER_ID_COL]]
    return added.drop_duplicates()


def load_offers(name: str) -> pd.DataFrame:
    return pd.read_parquet(resolve(name, "offers"), columns=OFFER_COLS)


def load_events(name: str) -> pd.DataFrame:
    events = pd.read_parquet(resolve(name, "events"))
    return events.loc[events.action == "add", EVENT_COLS].drop_duplicates(EVENT_ID_COL)


def load_similarities(name: str, columns: list[str] | None = None) -> pd.DataFrame:
    return pd.read_parquet(resolve(name, "similarities"), columns=columns)
