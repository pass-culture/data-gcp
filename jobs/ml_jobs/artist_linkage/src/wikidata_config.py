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

TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "queries"

# Human, musical group, duo, musical ensemble/collective
MUSIC_ENTITY_TYPES = ["wd:Q5", "wd:Q215380", "wd:Q216337", "wd:Q641066"]
PERSON_ENTITY_TYPES = ["wd:Q5"]


@dataclass(frozen=True)
class IdProperty:
    """A Wikidata external-ID property used to match and score candidate artists.

    `filter=False` excludes it from the "has at least one ID" entity pre-filter while
    still fetching its value and counting it towards the matching score (used for
    isni_id, which is too broad on its own to safely restrict the candidate set).
    """

    var: str
    property: str
    filter: bool = True


MUSIC_ID_PROPERTIES = [
    IdProperty("spotify_id", "wdt:P1902"),
    IdProperty("isni_id", "wdt:P213", filter=False),
    IdProperty("apple_music_id", "wdt:P2850"),
    IdProperty("deezer_id", "wdt:P2722"),
    IdProperty("genius_id", "wdt:P2373"),
    IdProperty("soundcloud_id", "wdt:P3040"),
]

BOOK_ID_PROPERTIES = [
    IdProperty("ibdbfw_id", "wdt:P5365"),
    IdProperty("babelio_id", "wdt:P3630"),
    IdProperty("goodreads_id", "wdt:P2963"),
    IdProperty("myanimelist_id", "wdt:P4084"),
]

MOVIE_ID_PROPERTIES = [
    IdProperty("imdb_id", "wdt:P345"),
    IdProperty("allocine_id", "wdt:P1266"),
]

GKG_ID_PROPERTIES = [
    IdProperty("gkg_id", "wdt:P2671"),
]

# Two-pass discovery+hydration templates (see QueryConfig.hydration_batch_size and
# extract_discovery.rq.j2 / extract_hydration.rq.j2 for the rationale). Reusable by
# any domain, not just gkg.
DISCOVERY_TEMPLATE = "extract_discovery.rq.j2"
HYDRATION_TEMPLATE = "extract_hydration.rq.j2"


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
    """

    template: str
    entity_types: list[str]
    id_properties: list[IdProperty] = field(default_factory=list)
    base_mode: Literal["scored", "grouped"] = "scored"
    hydration_batch_size: int | None = None


MUSIC_IDS_KEY = "music_ids"

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
        # cli/extract_from_wikidata.py's fetch_wikidata_qlever_csv_batch). Our real
        # constraint is QLever's ~30s time budget: 2,000 real entities measured at
        # 3.6s with HYDRATION_TEMPLATE, so 5,000 has a wide safety margin while
        # cutting the ~2.89M/5000 ≈ 578 batches needed (vs. ~1,450 at 2,000).
        template=DISCOVERY_TEMPLATE,
        entity_types=PERSON_ENTITY_TYPES,
        id_properties=GKG_ID_PROPERTIES,
        hydration_batch_size=5_000,
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
