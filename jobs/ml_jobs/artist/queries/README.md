# Wikidata extraction query templates

This folder holds the Jinja2 SPARQL templates that `cli/extract_from_wikidata.py`
renders and sends to QLever (`https://qlever.cs.uni-freiburg.de/api/wikidata`).
`src/wikidata_config.py` is the config layer: it defines, per domain (`music`,
`book`, `movie`, `gkg`, ...), which entity types and external-ID properties to
match, which template(s) to render them with, and (for domains that need it) how
to batch the work. Nothing here is domain-specific by construction — a new domain
is just a new entry in `QUERY_CONFIGS`.

`_macros.rq.j2` holds the Jinja macros shared by all four query templates below
(entity/ID-property filtering, the `matching_score` expression, the
single-valued label/description/image/Wikipedia fetch, and the isolated
multi-valued-attribute subquery pattern) — see "Shared macros" further down.
It isn't a template itself and is never passed to `render_query`.

There are two extraction strategies, used depending on how large and how
"rich" (in multi-valued data) a domain's candidate population is.

## Strategy 1: single-shot (`music`, `book`, `movie`, `music_ids`)

One query per target, fetching everything (labels, description, image,
Wikipedia link, birth date, professions, genres, languages, aliases, matching
score) in a single request. Used when the candidate population is small enough
(currently up to ~600K) that QLever completes it well within its execution time
budget.

- **`extract_artists.rq.j2`** — the shared template for `music`, `book`, and
  `movie`. Parameterized by `entity_types`, `id_properties`, and `base_mode`:
  - `base_mode="scored"` (book, movie): the base subquery only resolves
    `?matching_score`; single-valued attributes (labels, description,
    Wikipedia...) are fetched as separate top-level `OPTIONAL`s.
  - `base_mode="grouped"` (music): single-valued attributes are grouped
    together with the entity filter in one subquery, and no `?matching_score`
    column is produced — music's matching score is computed separately by
    `music_ids` (see below) and merged in client-side
    (`src/utils/wikidata_merge.py::merge_data`).

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
  candidates hydrate reliably today) but does **not** scale to a
  multi-million-candidate population; see Strategy 2.

- **`extract_artist_ids.rq.j2`** — `music`'s companion query, fetching each
  platform ID (Spotify, Deezer, Apple Music, Genius, SoundCloud, ISNI) and a
  computed `matching_score`, keyed by `music_ids` in `QUERY_CONFIGS`. Exists
  because `music`'s own query (`base_mode="grouped"`) doesn't compute a score;
  this one does, reusing `_macros.rq.j2`'s `entity_filter()` and
  `matching_score_expr()`, restricted to the ID properties themselves (no
  multi-valued attributes to isolate). It isn't just `extract_discovery.rq.j2`
  under another name: it wraps the ID-property fetch in `GROUP BY ?wikidata_id`
  + `MIN(...)` per property so an entity with more than one value on a given
  property (real and non-rare for `isni_id` in particular) still yields
  exactly one row — required because `merge_data` left-merges this query's
  output onto `music`'s own rows on `wikidata_id` and a duplicate row there
  would fan out `music`'s rows too. `extract_discovery.rq.j2` skips that
  aggregation on purpose (see its own entry below), so it can't stand in for
  this template as-is.

## Strategy 2: two-pass discovery + hydration (`gkg`)

Used when a domain's candidate population is too large, or contains entities
too rich, for a single-shot query — even the isolated-subquery pattern above —
to complete. `gkg` needs this: `wdt:P2671` (Google Knowledge Graph ID) matches
~2.9M humans, roughly 5× movie's population, and even reasonably-sized
subsets of it can contain individual entities documented richly enough to
blow QLever's cost budget on their own.

A `QueryConfig` opts into this by setting `hydration_batch_size`; `template`
then becomes the Pass 1 (discovery) template.

- **`extract_discovery.rq.j2`** (Pass 1) — a cheap, single query enumerating
  *every* entity matching the domain's entity filter (`_macros.rq.j2`'s
  `entity_filter()`), with its external-ID value(s) and matching score
  (`matching_score_expr()`). No multi-valued joins, no `GROUP_CONCAT`, no
  sort, and — unlike `extract_artist_ids.rq.j2` — deliberately no `GROUP BY`
  aggregation either: it's just an index scan and a hash join, which is
  exactly what QLever is built to do fast at scale. The tradeoff is that an
  entity with more than one value on a given ID property yields more than one
  row here; that's a non-issue for `gkg` (a single ID property) but is exactly
  why `music_ids` needs its own template instead of pointing at this one (see
  `extract_artist_ids.rq.j2` above).

- **`extract_hydration.rq.j2`** (Pass 2) — fetches the expensive multi-valued
  attributes for an explicit, client-supplied batch of entities
  (`wikidata_ids`), scoped via `_macros.rq.j2`'s `values_scope()`
  (`VALUES ?wikidata_id { wd:Q1 wd:Q2 ... }`) instead of re-deriving the
  entity filter (Pass 1 already found these entities). Like
  `extract_artists.rq.j2`, each multi-valued field is computed in its own
  subquery via the same `multi_valued_literal()`/`multi_valued_labeled()`
  macros — but here, re-stating the filter per field is *always* cheap,
  because it's just re-stating the same small `VALUES` list, not re-scanning
  a multi-million-candidate population.

  Orchestration: `cli/extract_from_wikidata.py`'s `extract` command drives the
  two-pass branch (runs Pass 1 once, chunks the discovered IDs into batches of
  `hydration_batch_size`, calls `hydrate_batch` per batch with
  `HYDRATION_BATCH_DELAY_SECONDS` between them, then inner-merges Pass 1 —
  id + matching_score — with the concatenated Pass 2 results on
  `wikidata_id`); the underlying two-pass logic it calls into lives in
  `src/utils/wikidata_extraction.py`, and the raw QLever HTTP fetch/retry
  client that in turn calls into lives in `src/utils/qlever.py`.
  - `hydrate_batch` bisects a batch and recurses whenever QLever rejects it
    as too expensive (`QLeverQueryTooExpensive`, detected via
    `_is_cost_rejection` — an HTTP 429 whose body names a timeout/cost
    estimate, as opposed to a transient error worth retrying unchanged), down
    to a single entity. A lone entity can, in principle, still carry enough
    aliases/professions/genres/languages on its own to exceed the budget even
    isolated; if QLever rejects it there too, it's skipped — logged as a
    `WARNING`, recorded in `dropped_ids`, and reported in `extract`'s
    end-of-task summary — rather than either blocking the whole extraction or
    silently dropping a wider batch around it. `dropped_ids` is a
    caller-owned accumulator threaded through every recursive call for
    exactly this reason: nothing is lost track of just because it happened
    deep in a recursion the caller never sees directly.
  - Requests use **POST**, not GET: a `VALUES`-scoped hydration query can run
    to tens of KB (5,000 entities ≈ 29KB), and GET puts the query in the URL —
    which hit a real `414 Request-URI Too Large` at ~30KB in testing. POST
    puts the query in the request body instead (`QLEVER_HEADERS`'
    `Content-Type: application/sparql-query` is exactly the SPARQL-protocol
    "query is the raw POST body" convention), with no such ceiling.

### Sizing `hydration_batch_size`

Because POST removes the URI-length ceiling, the real constraint is QLever's
own execution time budget for anonymous callers (empirically ~30s; see
`src/engine/Server.cpp`'s `verifyUserSubmittedQueryTimeout` in the QLever
source — an anonymous request can't exceed the server's configured default,
and there's no access token here to override it). `gkg` uses `5_000`: 2,000
real entities measured at 3.6s and 5,000 at 2.0s, both with wide margin under
that ~30s ceiling. Candidate count alone doesn't fully predict cost (a batch's
cost depends on how many of its entities are actually rich in multi-valued
data, not just how many entities there are), so treat a starting batch size
as a reasonable guess to validate empirically against real data before
trusting it, not a guarantee — `hydrate_batch`'s bisection self-heals at
runtime regardless, so getting it slightly wrong just costs some wasted
first-attempt time, not correctness.

### Resuming after a mid-run failure

Per the pipeline specification's "Checkpointing" section, `extract` persists
Pass 1's result and each hydrated Pass 2 batch to a local directory
(`CHECKPOINT_ROOT_DIR/<query_name>`, via `src/utils/wikidata_checkpoint.py`) as it
goes:
- `discovery.parquet` — Pass 1's output, so an Airflow-level retry of this
  same task doesn't re-run it.
- `batches/<index>.parquet` + `processed_batches.log` — one file per
  successfully hydrated batch, plus a log of which batch indexes are done.
  `extract` skips straight to loading the file for any index already in the
  log instead of re-fetching it.
- `dropped_ids.json` — the `dropped_ids` accumulator, so entities dropped in
  an earlier attempt are still reported (and still excluded) after a resume.

This works because of how the DAG runs `extract`: `InstallDependenciesOperator`
clones the repo once per DAG run (`vm_init`, before any `extract_{target}`
task), and an Airflow-level retry of `extract_{target}` re-SSHes into the
*same* VM without re-cloning — so the checkpoint directory survives exactly
the retries it needs to. The checkpoint is deleted once `extract` finishes
successfully, so a fresh month's run (a fresh VM) always starts clean, and
there's no cross-month state to go stale.

This only applies to two-pass targets — Strategy 1 already gets Airflow-level
retry safety "for free" (a single-shot query is cheap and fast enough that
redoing it from scratch isn't a real cost), and doesn't have a Pass 2 to
checkpoint into.

### What this deliberately does not implement

The two-pass pattern was adapted from a fuller pipeline specification that
also called for streaming each batch straight to disk instead of holding
results in memory. `gkg`'s full hydrated population is a few hundred MB in
memory at most, well within the extraction VM's RAM — the concern the spec
raises doesn't bite at this scale. Revisit if a future two-pass domain's
population is large enough that holding all its batches in memory
simultaneously becomes the actual bottleneck.

## Shared macros (`_macros.rq.j2`)

All four templates `{% import "_macros.rq.j2" as m %}` rather than each
redefining the same SPARQL fragments (as they used to — see git history prior
to this factoring). What's in there:

- `entity_filter(entity_types, id_properties)` — the entity-type +
  "has at least one filterable ID property" filter. Used by
  `extract_artists.rq.j2`, `extract_artist_ids.rq.j2`, and
  `extract_discovery.rq.j2` to discover candidates from scratch.
- `values_scope(wikidata_ids)` — the `VALUES ?wikidata_id { ... }` equivalent,
  used only by `extract_hydration.rq.j2`, which is handed an explicit id list
  instead of deriving one.
- `matching_score_expr(id_properties)` — the bare `IF(BOUND(?x), 1, 0) + ...`
  sum, used by `extract_artist_ids.rq.j2`'s outer `SELECT` list,
  `extract_artists.rq.j2`'s `base_mode="scored"` branch, and
  `extract_discovery.rq.j2`, each wrapping it in whatever their own context
  needs (a `SELECT`-list alias vs. a `BIND`).
- `labels_description_image()` / `wikipedia_url()` — the label
  (fr/en/mul, coalesced) + description + image, and Wikipedia-URL-via-
  `schema:about` blocks, shared verbatim by `extract_artists.rq.j2` (both
  `base_mode`s) and `extract_hydration.rq.j2`.
- `birth_date(scope="")` — the isolated `MIN(?birth_date)` subquery, shared
  by `extract_artists.rq.j2`'s `base_mode="scored"` branch (no `scope`: the
  entity is already bound by the base subquery there) and
  `extract_hydration.rq.j2` (scoped via `values_scope()`).
- `multi_valued_literal(...)` / `multi_valued_labeled(...)` — the isolated
  multi-valued-attribute subquery pattern described under Strategy 1 above,
  parameterized by predicate and (for the "labeled" variant, used by
  professions/genres/languages_spoken) the intermediate entity + label
  variable names. Shared by `extract_artists.rq.j2` and
  `extract_hydration.rq.j2`; each passes its own `scope` (`entity_filter()`
  or `values_scope()`).

What's deliberately **not** factored: `extract_discovery.rq.j2`'s lack of a
`GROUP BY`/`MIN` dedup step (see its entry above) and `extract_artists.rq.j2`'s
`base_mode="grouped"` vs `"scored"` branching aren't pushed into a shared
macro, because they're real behavioral differences between templates, not
copy-pasted boilerplate — a macro would just hide the difference behind a
flag instead of removing any duplication.

## Adding a new domain

1. Add its `IdProperty` list and entity types to `src/wikidata_config.py`.
2. Start with Strategy 1 (`QueryConfig(template="extract_artists.rq.j2", ...)`)
   — it's simpler, and has proven reliable up to movie's ~583K candidates.
3. Only reach for Strategy 2 if Strategy 1 actually fails against real QLever
   (a `QLeverQueryTooExpensive`/429 with a timeout-shaped `exception` body, or
   an outright HTTP 500 under load). When you do: set `hydration_batch_size`,
   pick a starting value informed by a live test against the new domain's own
   data (not just copied from `gkg`'s), and let `hydrate_batch`'s bisection
   handle whatever your starting guess gets wrong.
