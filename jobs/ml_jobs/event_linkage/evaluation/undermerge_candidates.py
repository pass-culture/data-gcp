"""Export under-merge candidates: clusters that nearly match another cluster.

Builds the cross-cluster similarity graph (combined sim >= threshold) and takes its
connected components. Each component of >= 2 events is a group of events that probably
should have been a single event.

Outputs:
    undermerge_offers.parquet : one row per offer in an under-merge group
    undermerge_pairs.parquet  : one row per candidate event-event bridge
"""

from pathlib import Path

import networkx as nx
import pandas as pd
import typer
from loguru import logger

from evaluation import data
from evaluation.clustering_metrics import (
    SEPARATION_MERGE_THRESHOLD,
    build_combined_similarity,
)

OFFER_OUT_COLS = [
    "merge_group_id",
    "event_id",
    "event_name",
    "cluster_size",
    "offer_id",
    "offer_name",
    "offer_description",
    "offer_subcategory_id",
    "best_external_sim",
    "best_external_event_id",
]

DEFAULT_OFFERS_OUT = Path(__file__).resolve().parent / "undermerge_offers.parquet"
DEFAULT_PAIRS_OUT = Path(__file__).resolve().parent / "undermerge_pairs.parquet"


def main(
    threshold: float = typer.Option(SEPARATION_MERGE_THRESHOLD, help="Min bridge sim."),
    links: str = typer.Option(data.LINKS_PARQUET, help="Link delta parquet."),
    offers_parquet: str = typer.Option(data.OFFERS_PARQUET, help="Offers parquet."),
    events_parquet: str = typer.Option(data.EVENTS_PARQUET, help="Events parquet."),
    similarities: str = typer.Option(data.SIMILARITIES_PARQUET, help="Similarities."),
    out_offers: str = typer.Option(str(DEFAULT_OFFERS_OUT), help="Offers output."),
    out_pairs: str = typer.Option(str(DEFAULT_PAIRS_OUT), help="Event pairs output."),
) -> None:
    links_df = data.load_links(links)
    cluster_of = links_df.set_index("offer_id")["event_id"]
    sizes = links_df.groupby("event_id")["offer_id"].size()

    offers = data.load_offers(offers_parquet)
    events = data.load_events(events_parquet)

    long = build_combined_similarity(data.load_similarities(similarities))
    long = long[long.i.isin(cluster_of.index) & long.j.isin(cluster_of.index)]
    long["ca"] = long.i.map(cluster_of)
    long["cb"] = long.j.map(cluster_of)
    is_bridge = (long.ca != long.cb) & (long.sim >= threshold) & (long.i < long.j)
    cross = long[is_bridge].copy()
    logger.info(f"{len(cross)} cross-cluster bridges >= {threshold}.")

    g = nx.Graph()
    g.add_edges_from(zip(cross.ca, cross.cb, strict=True))
    comps = [c for c in nx.connected_components(g) if len(c) >= 2]
    event_group = {ev: f"merge_{i}" for i, comp in enumerate(comps) for ev in comp}
    logger.info(f"{len(comps)} under-merge groups covering {len(event_group)} events.")

    # Best external match per offer, to show WHICH offers are the near-twins.
    ext = long[long.ca != long.cb]
    best_ext = (
        ext.sort_values("sim", ascending=False)
        .drop_duplicates("i")
        .set_index("i")[["sim", "cb"]]
        .rename(columns={"sim": "best_external_sim", "cb": "best_external_event_id"})
    )

    grp_events = pd.Series(event_group, name="merge_group_id")
    grp_events = grp_events.rename_axis("event_id").reset_index()
    off = (
        links_df[links_df.event_id.isin(event_group)]
        .merge(grp_events, on="event_id")
        .merge(offers, on="offer_id", how="left")
        .merge(events, on="event_id", how="left")
    )
    off["cluster_size"] = off.event_id.map(sizes)
    off = off.merge(best_ext, left_on="offer_id", right_index=True, how="left")
    off = off.sort_values(
        ["merge_group_id", "event_id", "best_external_sim"],
        ascending=[True, True, False],
    )[OFFER_OUT_COLS]

    # Candidate event-event pairs, keeping only the strongest bridge per pair.
    cross["pair"] = cross.apply(lambda r: tuple(sorted((r.ca, r.cb))), axis=1)
    strongest = cross.sort_values("sim", ascending=False).drop_duplicates("pair")
    pairs = pd.DataFrame(
        {
            "event_id_a": [p[0] for p in strongest.pair],
            "event_id_b": [p[1] for p in strongest.pair],
            "bridge_sim": strongest.sim.to_numpy(),
            "bridge_offer_a": strongest.i.to_numpy(),
            "bridge_offer_b": strongest.j.to_numpy(),
        }
    )
    pairs["merge_group_id"] = pairs.event_id_a.map(event_group)
    ev_a = events.rename(
        columns={
            "event_id": "event_id_a",
            "event_name": "event_name_a",
            "event_description": "event_description_a",
        }
    )
    ev_b = events.rename(
        columns={
            "event_id": "event_id_b",
            "event_name": "event_name_b",
            "event_description": "event_description_b",
        }
    )
    pairs = pairs.merge(ev_a, on="event_id_a", how="left")
    pairs = pairs.merge(ev_b, on="event_id_b", how="left")

    off.to_parquet(out_offers, index=False)
    pairs.to_parquet(out_pairs, index=False)
    logger.success(
        f"Wrote {len(off)} offers across {off.event_id.nunique()} events / "
        f"{off.merge_group_id.nunique()} groups -> {out_offers}\n"
        f"Wrote {len(pairs)} candidate event pairs -> {out_pairs}"
    )
    largest = (
        off.groupby("merge_group_id")
        .agg(events=("event_id", "nunique"), offers=("offer_id", "size"))
        .sort_values("offers", ascending=False)
        .head(10)
    )
    logger.info("Largest under-merge groups (events, offers):\n" + largest.to_string())


if __name__ == "__main__":
    typer.run(main)
