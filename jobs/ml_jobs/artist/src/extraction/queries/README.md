# Wikidata extraction query templates

This folder holds the Jinja2 SPARQL templates that `cli/extraction.py` renders
and sends to QLever (`https://qlever.cs.uni-freiburg.de/api/wikidata`).
`src/extraction/wikidata_config.py` is the config layer: it defines, per domain
(`music`, `book`, `movie`, `gkg`, ...), which entity types and external-ID
properties to match and which template to render them with — the
entity-type/ID-property values themselves live in
`src/extraction/constants.py`. Nothing here is
domain-specific by construction — a new domain is just a new entry in
`QUERY_CONFIGS`.

`_macros.rq.j2` holds the Jinja macros shared by the query templates below
(entity/ID-property filtering, the `matching_score` expression, the
single-valued label/description/image/Wikipedia fetch, and the isolated
multi-valued-attribute subquery pattern) — see "Shared macros" further down.
It isn't a template itself and is never passed to `render_query`.

## Templates

One query per target. Targets with `sharded=True` in `QUERY_CONFIGS` (music,
book, movie) are sent as 10 requests instead of one: `entity_filter()` takes a
`shard` digit and keeps only Q-ids ending with it
(`FILTER(STRENDS(STR(?wikidata_id), "<d>"))`), and `extract_query`
concatenates the 10 results. QLever streams its results and gives up on a
response that takes too long to send ("Operation timed out"), which these full
queries hit. The last digit splits entities into disjoint, roughly equal
slices, with no pagination state. Because every subquery reuses
`entity_filter()`, each of them shrinks too, not just the final join.

- **`extract_artists.rq.j2`** — the shared template for `music`, `book`, and
  `movie`. Parameterized by `entity_types`, `id_properties`, and `base_mode`:
  - `base_mode="scored"` (book, movie): the base subquery only resolves
    `?matching_score`; single-valued attributes (labels, description,
    Wikipedia...) are fetched as separate top-level `OPTIONAL`s.
  - `base_mode="grouped"` (music): single-valued attributes are grouped
    together with the entity filter in one subquery, and no `?matching_score`
    column is produced — music's matching score is computed separately by
    `music_ids` (see below) and merged in client-side
    (`src/extraction/wikidata_merge.py::merge_data`).

  Every multi-valued field (aliases_fr, aliases_en, professions, genres,
  languages_spoken) is computed in its **own** subquery, via `_macros.rq.j2`'s
  `multi_valued_literal()`/`multi_valued_labeled()` macros — each re-applies
  the same `entity_filter()` scope inside its own subquery — rather than
  joined together in one scope. This is deliberate: joining
  multiple multi-valued `OPTIONAL`s in one scope makes SPARQL compute their
  full cross-product per entity (aliases_fr × aliases_en × professions ×
  genres × languages) before `GROUP_CONCAT(DISTINCT ...)` collapses it back
  down — for a richly-documented entity that's a real cost, and it's exactly
  what caused the QLever HTTP 429/500 timeouts that an earlier version of
  these queries used to hit (see git history: `fix(sparql): optimize Wikidata
  extraction queries for QLever`). Isolating each field avoids the
  cross-product entirely, at the cost of re-deriving the entity filter once
  per field — a cost that's fine at this population size (movie's ~583K
  candidates complete reliably today) but does **not** scale to a
  multi-million-candidate population (see `gkg` below).

- **`extract_artist_ids.rq.j2`** — fetches only each entity's external-ID
  values and a computed `matching_score`, reusing `_macros.rq.j2`'s
  `entity_filter()` and `matching_score_expr()` — no labels and no
  multi-valued attributes, so it stays fast even on a multi-million-candidate
  population. It wraps the ID-property fetch in `GROUP BY ?wikidata_id` +
  `MIN(...)` per property so an entity with more than one value on a given
  property (real and non-rare for `isni_id` in particular) still yields
  exactly one row. Used by two targets:
  - `music_ids` — `music`'s companion query, fetching each platform ID
    (Spotify, Deezer, Apple Music, Genius, SoundCloud, ISNI). Exists because
    `music`'s own query (`base_mode="grouped"`) doesn't compute a score; the
    one-row-per-entity guarantee is required because `merge_data` left-merges
    this query's output onto `music`'s own rows on `wikidata_id`, and a
    duplicate row there would fan out `music`'s rows too.
  - `gkg` — `wdt:P2671` (Google Knowledge Graph ID) matches ~2.9M humans,
    roughly 5× movie's population, far too large for `extract_artists.rq.j2`.
    It doesn't need to be: downstream only uses the boolean `gkg` column
    `merge_data` derives from this target's `wikidata_id`s (a notability
    tie-breaker in `src/linkage/matching.py` and
    `src/linkage/metadata_refresh.py`), never its attributes. An entity found
    only by `gkg` therefore has no name and is dropped by `postprocess_data`
    — which is fine, since linkage only ever matches against
    `music`/`book`/`movie` entities.

## Shared macros (`_macros.rq.j2`)

Both templates `{% import "_macros.rq.j2" as m %}` rather than each
redefining the same SPARQL fragments (as they used to — see git history prior
to this factoring). What's in there:

- `entity_filter(entity_types, id_properties)` — the entity-type +
  "has at least one filterable ID property" filter. Used by both templates.
- `matching_score_expr(id_properties)` — the bare `IF(BOUND(?x), 1, 0) + ...`
  sum, used by `extract_artist_ids.rq.j2`'s outer `SELECT` list and
  `extract_artists.rq.j2`'s `base_mode="scored"` branch, each wrapping it in
  whatever their own context needs (a `SELECT`-list alias vs. a `BIND`).
- `labels_description_image()` / `wikipedia_url()` — the label
  (fr/en/mul, coalesced) + description + image, and Wikipedia-URL-via-
  `schema:about` blocks, shared verbatim by both of `extract_artists.rq.j2`'s
  `base_mode`s.
- `birth_date()` — the isolated `MIN(?birth_date)` subquery, used by
  `extract_artists.rq.j2`'s `base_mode="scored"` branch (unscoped: the
  entity is already bound by the base subquery there).
- `multi_valued_literal(...)` / `multi_valued_labeled(...)` — the isolated
  multi-valued-attribute subquery pattern described under
  `extract_artists.rq.j2` above, parameterized by `scope` (the pre-rendered
  `entity_filter()`), predicate and (for the "labeled" variant, used by
  professions/genres/languages_spoken) the intermediate entity + label
  variable names.

What's deliberately **not** factored: `extract_artists.rq.j2`'s
`base_mode="grouped"` vs `"scored"` branching isn't pushed into a shared
macro, because it's a real behavioral difference, not copy-pasted
boilerplate — a macro would just hide the difference behind a flag instead of
removing any duplication.

## Adding a new domain

1. Add its `IdProperty` list and entity types to `src/extraction/constants.py`,
   then reference them from a new `QUERY_CONFIGS` entry in
   `src/extraction/wikidata_config.py`.
2. Use `QueryConfig(template="extract_artists.rq.j2", ...)` — it has proven
   reliable up to movie's ~583K candidates. If the population is much larger
   and only the IDs are needed downstream, use `extract_artist_ids.rq.j2`
   instead, like `gkg`.
3. Add the new key to `EXTRACTION_TARGETS` in
   `orchestration/dags/jobs/ml/artist_wikidata_dump.py`.
