"""Per-domain config used to render the Jinja SPARQL templates in `queries/`.

Each Wikidata extraction query (music, book, movie, gkg...) shares the same overall
shape (entity-type filter, external-ID matching, multi-valued attribute subqueries)
but differs in which Wikidata entity types and external-ID properties it targets.
This module captures those differences so the templates stay DRY.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from jinja2 import Environment, FileSystemLoader

from src.extraction.constants import (
    BOOK_ID_PROPERTIES,
    DISCOVERY_TEMPLATE,
    GKG_ID_PROPERTIES,
    HYDRATION_BATCH_SIZE,
    MOVIE_ID_PROPERTIES,
    MUSIC_ENTITY_TYPES,
    MUSIC_ID_PROPERTIES,
    MUSIC_IDS_KEY,
    PERSON_ENTITY_TYPES,
    IdProperty,
)

TEMPLATE_DIR = Path(__file__).resolve().parent / "queries"


@dataclass(frozen=True)
class QueryConfig:
    """Config used to render one query from a template in `queries/`.

    `base_mode` only applies to the "extract_artists.rq.j2" template:
      - "scored": the base subquery only resolves `?matching_score` from
        `id_properties`; single-valued attributes (labels, description, wikipedia...)
        are fetched as separate top-level OPTIONALs.
      - "grouped": single-valued attributes are grouped together with the entity
        filter in one subquery, and no `?matching_score` column is produced (used by
        `music`, whose matching score is computed separately by `music_ids`).

    `hydration_batch_size`, when set, switches `extract` to the two-pass
    discovery+hydration pattern instead of a single-shot query: `template` becomes
    the Pass 1 discovery query (DISCOVERY_TEMPLATE), which cheaply enumerates every
    matching entity, and Pass 2 hydrates them in VALUES-scoped batches of this size
    (HYDRATION_TEMPLATE) — for a domain whose candidate population is too large, or
    contains entities too rich, for a single-shot query to complete. Reusable by
    any domain, not just `gkg`.

    `optional`, when true, means a genuinely empty/missing result for this target
    is expected, not a failure: `extract` skips saving a raw file instead of
    raising, and `merge` skips it instead of hard-failing the whole merge. Used by
    `music_ids`, whose own query can legitimately return nothing (see
    `merge_data`'s pre-merge step in src/extraction/wikidata_merge.py).
    """

    template: str
    entity_types: list[str]
    id_properties: list[IdProperty] = field(default_factory=list)
    base_mode: Literal["scored", "grouped"] = "scored"
    hydration_batch_size: int | None = None
    optional: bool = False


QUERY_CONFIGS: dict[str, QueryConfig] = {
    "music": QueryConfig(
        template="extract_artists.rq.j2",
        entity_types=MUSIC_ENTITY_TYPES,
        id_properties=MUSIC_ID_PROPERTIES,
        base_mode="grouped",
    ),
    MUSIC_IDS_KEY: QueryConfig(
        template="extract_artist_ids.rq.j2",
        entity_types=MUSIC_ENTITY_TYPES,
        id_properties=MUSIC_ID_PROPERTIES,
        optional=True,
    ),
    "book": QueryConfig(
        template="extract_artists.rq.j2",
        entity_types=PERSON_ENTITY_TYPES,
        id_properties=BOOK_ID_PROPERTIES,
    ),
    "movie": QueryConfig(
        template="extract_artists.rq.j2",
        entity_types=PERSON_ENTITY_TYPES,
        id_properties=MOVIE_ID_PROPERTIES,
    ),
    "gkg": QueryConfig(
        # Two-pass discovery+hydration: gkg's ~2.9M-candidate population (wdt:P2671
        # is far broader than movie's IMDb/Allociné) is too large for a single-shot
        # query, and guessing safe ID-range batch widths doesn't work either —
        # density varies ~90x across the ID space, and even a single richly-aliased
        # entity can blow the budget regardless of batch width. Pass 1 cheaply
        # enumerates every match (2.89M rows in 18s, live-measured); Pass 2
        # hydrates them in VALUES-scoped batches instead.
        # 5000, not the 200-500 usually recommended for VALUES batches: that
        # guidance is about GET URI-length limits, which doesn't apply here since
        # extract uses POST (query in the body, no URI-length ceiling — see
        # src/extraction/qlever.py's fetch_wikidata_qlever_csv_batch). Our real
        # constraint is QLever's ~30s time budget: 2,000 real entities measured at
        # 3.6s with HYDRATION_TEMPLATE, so 5,000 has a wide safety margin while
        # cutting the ~2.89M/5000 ≈ 578 batches needed (vs. ~1,450 at 2,000).
        template=DISCOVERY_TEMPLATE,
        entity_types=PERSON_ENTITY_TYPES,
        id_properties=GKG_ID_PROPERTIES,
        hydration_batch_size=HYDRATION_BATCH_SIZE,
    ),
}

# autoescape is off on purpose: these templates render SPARQL query text (not HTML)
# from the static QUERY_CONFIGS below, not from user input. HTML-escaping would
# corrupt the query syntax (e.g. the `<...>` IRIs and `&` in Wikidata property URIs).
_jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
    autoescape=False,
)


def render_query(
    query_name: str,
    wikidata_ids: list[str] | None = None,
    template: str | None = None,
) -> str:
    """Render a query for `query_name`.

    `wikidata_ids`, for the Pass 2 hydration templates, is a list of bare IDs
    (e.g. "Q123") to inject as a `VALUES ?wikidata_id { wd:Q123 ... }` clause —
    formatted into CURIEs here so callers only ever deal with bare IDs (matching
    `extract_wikidata_id`'s output).
    """
    config = QUERY_CONFIGS[query_name]
    rendered_template = _jinja_env.get_template(template or config.template)
    return rendered_template.render(
        entity_types=config.entity_types,
        id_properties=config.id_properties,
        base_mode=config.base_mode,
        wikidata_ids=(
            [f"wd:{wikidata_id}" for wikidata_id in wikidata_ids]
            if wikidata_ids
            else None
        ),
    )
