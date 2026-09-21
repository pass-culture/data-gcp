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
    """

    template: str
    entity_types: list[str]
    id_properties: list[IdProperty] = field(default_factory=list)
    base_mode: Literal["scored", "grouped"] = "scored"


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
        template="extract_artists.rq.j2",
        entity_types=PERSON_ENTITY_TYPES,
        id_properties=GKG_ID_PROPERTIES,
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


def render_query(query_name: str) -> str:
    config = QUERY_CONFIGS[query_name]
    template = _jinja_env.get_template(config.template)
    return template.render(
        entity_types=config.entity_types,
        id_properties=config.id_properties,
        base_mode=config.base_mode,
    )
