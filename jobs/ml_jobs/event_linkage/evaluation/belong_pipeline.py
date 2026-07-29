"""Two-stage 'belong' labelling of event-linkage clusters.

Stage 1 (free, deterministic) splits clusters into certainly-good and to-be-judged
using the pairwise similarity table. Stage 2 sends only the to-be-judged clusters to
a Gemini judge on Vertex, which labels each offer belong in {true, false, unknown}.
See the README in this directory for the rules the judge applies.

Output columns: belong, belong_confidence, belong_reason, belong_source.

Run:
    python evaluation/belong_pipeline.py --stage1-only
    python evaluation/belong_pipeline.py --limit 10
    python evaluation/belong_pipeline.py --project passculture-data-ehp
"""

from __future__ import annotations

from enum import StrEnum
from itertools import combinations
from pathlib import Path

import pandas as pd
import typer
from loguru import logger
from pydantic import BaseModel, Field

from evaluation import data

# Stage-1 homogeneity thresholds: both must hold for a pair to be "consistent".
T_NAME = 90
T_DESC = 90

MODEL = "gemini-2.5-flash"
LOCATION = "global"
MAX_DESC = 800

DEFAULT_OUT = Path(__file__).resolve().parent / "belong_results.parquet"


def _s(x) -> str:
    """NaN/None-safe string (NaN is truthy, so `x or ''` is wrong)."""
    return str(x).strip() if pd.notna(x) else ""


def load_merged(links: str, offers: str, events: str) -> pd.DataFrame:
    """Net linkage state joined to offer + event metadata."""
    merged = (
        data.load_links(links)
        .merge(data.load_offers(offers), on="offer_id", how="left")
        .merge(data.load_events(events), on="event_id", how="left")
    )
    merged["cluster_size"] = merged.groupby("event_id")["offer_id"].transform("size")
    return merged


def build_pair_lookup(
    similarities: str, linked_offers: set[str]
) -> dict[frozenset, tuple[int, int]]:
    """frozenset({offer_a, offer_b}) -> (name_similarity, description_similarity)."""
    sim = data.load_similarities(
        similarities,
        columns=[
            "offer_id_1",
            "offer_id_2",
            "name_similarity",
            "description_similarity",
        ],
    )
    sim = sim[sim.offer_id_1.isin(linked_offers) & sim.offer_id_2.isin(linked_offers)]
    rows = sim.itertuples(index=False)
    lookup = {frozenset((a, b)): (int(n), int(d)) for a, b, n, d in rows}
    logger.info(f"Loaded {len(lookup)} intra-linked similarity pairs.")
    return lookup


def stage1(merged: pd.DataFrame, lookup: dict) -> pd.DataFrame:
    """Label a cluster 'true' iff every intra-cluster pair is present and strong."""
    belong, reason = {}, {}
    for event_id, g in merged.groupby("event_id"):
        ids = sorted(g.offer_id)
        if len(ids) == 1:
            belong[event_id], reason[event_id] = "true", "singleton"
            continue
        homogeneous, why = True, f"all pairs name>={T_NAME} & desc>={T_DESC}"
        for a, b in combinations(ids, 2):
            v = lookup.get(frozenset((a, b)))
            if v is None:
                homogeneous, why = False, "missing pair (no direct similarity edge)"
                break
            n, d = v
            if not (n >= T_NAME and d >= T_DESC):
                homogeneous, why = False, f"weak pair (name={n}, desc={d})"
                break
        belong[event_id] = "true" if homogeneous else "unknown"
        reason[event_id] = why

    merged = merged.copy()
    merged["belong"] = merged.event_id.map(belong)
    merged["belong_confidence"] = merged.belong.map({"true": 1.0}).astype("float")
    merged["belong_reason"] = merged.event_id.map(reason)
    merged["belong_source"] = "similarity"
    return merged


class Belong(StrEnum):
    true = "true"
    false = "false"
    unknown = "unknown"


class OfferVerdict(BaseModel):
    offer_id: str
    belong: Belong = Field(description="true=belongs, false=not, unknown=not sure.")
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str = Field(description="One sentence; for 'unknown', explain the doubt.")


class EventVerdict(BaseModel):
    offers: list[OfferVerdict]


SYSTEM = (
    "You evaluate a French cultural-offers catalogue (pass Culture). An EVENT is "
    "one and the same show/spectacle, which may run on different dates or in "
    "different venues. Given an EVENT (name + description) and the OFFERS grouped "
    "under it, decide for EACH offer whether it belongs to the same event. Reply "
    "per offer with belong=true (same event), belong=false (clearly a different "
    "event), or belong=unknown when the evidence is insufficient or contradictory — "
    "and in that case the 'reason' MUST explain the doubt.\n\n"
    "CRITICAL — FESTIVALS: A festival or offers sharing a common festival "
    "name/brand, e.g. 'Festival Jeunes Talents - ...', 'Les Estivales de Saône - "
    "...') is ONE event that hosts MANY different artists, concerts, programmes and "
    "themes across several dates and venues. For a festival, all offers that share "
    "the SAME festival belong to the SAME event (belong=true) EVEN IF each offer "
    "features a completely different artist, programme, or theme — the individual "
    "artist/programme is just one act inside the festival, not a separate event. "
    "Example: 'Festival Jeunes Talents - Gracias a la vida' and 'Festival Jeunes "
    "Talents - Les Hautbois de la Chambre' are the SAME festival -> BOTH "
    "belong=true, despite different artists. Only mark an offer false if it clearly "
    "belongs to a DIFFERENT festival or is unrelated to this festival.\n\n"
    "For a single concert/show that is NOT a festival, a different headline artist "
    "or production DOES mean a different event (belong=false) — e.g. 'Piche' vs "
    "'Pierre de Maere', or 'Casse-Noisette' vs 'Le Lac des Cygnes'.\n\n"
    "For subcategory SPECTACLE_REPRESENTATION names are generic; rely on "
    "descriptions if available. If you are unsure if the offer fits the event, mark "
    "it unknown and explain the doubt.\n\n"
    "Sometimes the event name is generic and the description is missing or "
    "unhelpful. In that case, mark the offers as unknown and explain the doubt.\n\n"
    "Do not be too strict, do not put offers as unknown if you think they most "
    "probably belong to the same event.\n\n"
    "Return JSON matching the schema."
)


def build_prompt(g: pd.DataFrame) -> str:
    subcat = _s(g.offer_subcategory_id.iloc[0]) or "UNKNOWN"
    lines = [f"Subcategory: {subcat}", f"OFFERS ({len(g)}):"]
    for r in g.itertuples():
        d = _s(r.offer_description).replace("\n", " ")[:MAX_DESC]
        lines += [
            f"- offer_id={r.offer_id}",
            f"  name: {_s(r.offer_name)}",
            f"  description: {d or '(none)'}",
        ]
    return "\n".join(lines)


def stage2(merged: pd.DataFrame, project: str, limit: int) -> pd.DataFrame:
    """Judge the clusters stage 1 left as 'unknown', one cluster per call."""
    from google import genai
    from google.genai import types

    client = genai.Client(vertexai=True, project=project, location=LOCATION)
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM,
        response_mime_type="application/json",
        response_schema=EventVerdict,
        temperature=0.0,
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )

    unknown = merged.loc[merged.belong == "unknown", "event_id"]
    unknown_events = unknown.unique().tolist()
    if limit:
        unknown_events = unknown_events[:limit]
    logger.info(f"Stage 2: judging {len(unknown_events)} unknown clusters.")

    verdicts = {}
    for i, eid in enumerate(unknown_events, 1):
        g = merged[merged.event_id == eid]
        try:
            resp = client.models.generate_content(
                model=MODEL, contents=build_prompt(g), config=config
            )
            for o in resp.parsed.offers:
                verdicts[(eid, o.offer_id)] = (o.belong.value, o.confidence, o.reason)
        except Exception as exc:
            logger.error(f"cluster {eid} failed: {exc}")
        if i % 25 == 0:
            logger.info(f"  judged {i}/{len(unknown_events)}")

    merged = merged.copy()
    verdict_cols = ["belong", "belong_confidence", "belong_reason", "belong_source"]
    for (eid, oid), (b, c, r) in verdicts.items():
        m = (merged.event_id == eid) & (merged.offer_id == oid)
        merged.loc[m, verdict_cols] = [b, c, r, "llm"]
    return merged


app = typer.Typer(add_completion=False)


def _report(merged: pd.DataFrame, stage: str) -> None:
    clusters = merged.drop_duplicates("event_id").belong.value_counts()
    logger.info(f"[{stage}] clusters by belong:\n" + clusters.to_string())
    offers = merged.belong.value_counts()
    logger.info(f"[{stage}] offers by belong:\n" + offers.to_string())


@app.command()
def main(
    project: str = typer.Option("passculture-data-ehp", help="GCP project for Vertex."),
    limit: int = typer.Option(0, help="Cap unknown clusters judged (0=all)."),
    links: str = typer.Option(data.LINKS_PARQUET, help="Link delta parquet."),
    offers: str = typer.Option(data.OFFERS_PARQUET, help="Offers parquet."),
    events: str = typer.Option(data.EVENTS_PARQUET, help="Event series delta parquet."),
    similarities: str = typer.Option(data.SIMILARITIES_PARQUET, help="Similarities."),
    output: str = typer.Option(str(DEFAULT_OUT), help="Where to write results."),
    *,
    stage1_only: bool = typer.Option(False, "--stage1-only", help="Skip the LLM."),
) -> None:
    merged = load_merged(links, offers, events)
    lookup = build_pair_lookup(similarities, set(merged.offer_id))
    merged = stage1(merged, lookup)
    _report(merged, "stage1")

    if not stage1_only:
        merged = stage2(merged, project, limit)
        _report(merged, "final")

    merged.to_parquet(output, index=False)
    logger.success(f"Wrote {output}")


if __name__ == "__main__":
    app()
