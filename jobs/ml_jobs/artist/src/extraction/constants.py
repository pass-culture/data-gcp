from dataclasses import dataclass

# Wikidata entity types (Human, musical group, duo, musical ensemble/collective)
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

MUSIC_IDS_KEY = "music_ids"

WIKIDATA_ENTITY_PREFIX = r"https?://www\.wikidata\.org/entity/"

# QLever (https://qlever.cs.uni-freiburg.de/api/wikidata) HTTP client settings.
QLEVER_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"
QLEVER_HEADERS = {
    "Accept": "text/csv",
    "Content-Type": "application/sparql-query",
    # Same identification string used for other external APIs (see
    # src.similarity.constants.WIKIMEDIA_REQUEST_HEADER) — good practice for any
    # shared third-party endpoint, and QLever's own docs ask for one explicitly.
    "User-Agent": "PassCulture/1.0 (https://passculture.app; contact@passculture.app) Python/requests",
}
