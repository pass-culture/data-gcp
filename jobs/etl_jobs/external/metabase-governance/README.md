# metabase-governance

Daily job that governs the analytics Metabase: archives stale cards, hard-archives long-dormant ones, prunes empty dashboards/collections, syncs collection permissions from a versioned config, and classifies the collection tree into a squad/tier/certified taxonomy.

CLI is one Typer app (`main.py`) with five commands:

| Command | What it does |
|---|---|
| `archive` | Runs the three-tier archiving pipeline (soft → hard → cleanup). |
| `stats` | Prints the same pre-flight stats as `archive` but mutates nothing. |
| `permissions` | Reconciles Metabase collection permissions with the YAML config. |
| `dependencies` | Exports card → table dependencies to BigQuery (used by the legacy table-migration tool in `metabase/`). |
| `taxonomy` | Classifies every collection into `{squad, tier, certified, in_scope}` from the rules in `config/taxonomy/{env}.yaml` and writes `int_metabase_<env>.collection_taxonomy` (consumed by the dbt `int_metabase__asset_catalog`). |

All commands read environment-specific config from `config/{archiving,permissions,taxonomy}/{staging,production}.yaml`, picked by `ENV_SHORT_NAME`. `dev` and `stg` both use `staging.yaml`.

## 1. Archiving

Three independent passes, run in this order:

### 1a. Soft archive — move + rename `[Archive] - …`

Reads the dbt model `int_metabase__activity` (one row per active card with its activity stats and the lower-cased ancestor collection chain).

A card matches a rule when **all** are true:

- any name in `ancestor_collection_names` matches the rule's SQL-LIKE `ancestor_title_pattern` (case-insensitive),
- card is older than `min_age_days` (`card_creation_date`),
- card hasn't been edited in the last `min_days_since_update` days (`card_update_date`) — protects actively-edited cards even with low traffic,
- views in the rolling `views_window_months` window are strictly less than `max_views`.

**First-rule-wins**: a card matching multiple rules is archived under the rule listed first in YAML.

For each candidate the job:
1. Calls `POST /api/card/collections` to move the card to `archive_collection_id`.
2. **Only if the move succeeded**, renames the card with `[Archive] - …` prefix (otherwise we'd leave a renamed zombie in the source folder).
3. Logs success/failure to `archiving_log` BigQuery table.

Cards that have failed in the last `soft_archive_failure_cooldown_days` days are skipped automatically — Metabase remote-sync rejections etc. don't get retried daily.

Personal collections are never touched: the dbt model already filters `personal_owner_id IS NULL`.

### 1b. Hard archive — `archived = true` for `[Archive]` cards stale > N days

SQL-driven candidate selection on `raw.metabase_report_card`:

```sql
WHERE REGEXP_CONTAINS(card_name, r'(?i){name_pattern}')
  AND date(updated_at) < date_sub(current_date(), interval {min_days_since_update} day)
  AND coll_root.own_personal_owner_id IS NULL
  AND root_coll.personal_owner_id IS NULL
```

The double personal-collection filter excludes cards whose **immediate** collection is personal AND cards whose **root ancestor** is personal — Metabase only sets the flag on the root, descendants inherit personal-ness via their location path.

Each candidate gets `archived: true` set via `PUT /api/card/{id}` after a per-card check that it isn't already archived.

### 1c. Empty cleanup — dashboards + collections

Walks every root in `empty_cleanup.scan_root_collection_ids` recursively via `GET /api/collection/{id}/items`.

A **dashboard is dead** if it has zero dashcards or every dashcard is archived. Virtual dashcards (text/heading/link/iframe — items with `card: null`) count as live content. The check reads both `dashcards` (modern) and `ordered_cards` (legacy) keys.

A **collection is empty** if `GET /api/collection/{id}/items` returns no items.

Both are archived only if `created_at < now - min_age_days` AND, when set, `updated_at < now - min_days_since_update`. If the listing endpoint omits `created_at`/`updated_at` (varies by Metabase version), the job falls back to a single full-object fetch.

The configured roots themselves are never archived (they're added to an exclude set).

### Archiving config (`config/archiving/{staging,production}.yaml`)

```yaml
archive_collection_id: 610            # 717 in staging
max_cards_to_archive: 50              # cap per soft-archive run
soft_archive_failure_cooldown_days: 30  # skip cards that failed in last N days

soft_archive_rules:
  - name: thematic
    ancestor_title_pattern: "%suivi par thématiques%"
    min_age_days: 60
    min_days_since_update: 60
    views_window_months: 6
    max_views: 1                      # strict less-than → 0 views

  - name: adhoc
    ancestor_title_pattern: "%adhoc%"
    min_age_days: 15
    min_days_since_update: 15
    views_window_months: 3
    max_views: 5

hard_archive_cards:
  name_pattern: "^\\[Archive\\]"
  min_days_since_update: 30
  max_cards: 50

empty_cleanup:
  min_age_days: 15
  min_days_since_update: 15
  scan_root_collection_ids:
    - 608  # internal
    - 607  # external
    - 606  # operational
    - 610  # archive
```

### Pre-flight stats (auto-printed by `archive`, standalone via `stats`)

Reads BQ + activity table to show, before any mutation:

- Total cards split by scope (`global`, `global_root`, `personal`).
- Soft-archive candidates total + per-rule + per top-level folder.
- Hard-archive candidates total + per top-level folder (personal excluded).
- Cleanup roots being scanned (per-item counts only visible in `--dry-run`).

```bash
uv run python main.py stats
```

### Dry-run

Walks every step but skips all PUT/POST mutations and BQ log writes; prints `[DRY-RUN] Would …` per item.

```bash
uv run python main.py archive --dry-run
```

### Sandbox testing against an alternate activity table

Useful before deploying a new dbt model — point at a custom table without code changes:

```bash
uv run python main.py archive --dry-run \
  --dataset-name <sandbox> --table-name int_metabase__activity_test
```

### Tracking — `int_metabase_<env>.archiving_log`

Every move/hard-archive attempt writes a row (success and failure):

| column | meaning |
|---|---|
| `id` | card id |
| `object_type` | `card` (soft) or `card_hard_archive` |
| `status` | `'success'`, `'error'`, or `null` on hard API failure |
| `new_collection_id` / `previous_collection_id` | target / source folder |
| `archived_at` | timestamp of the run |
| `parent_folder` | rule name (`thematic`, `adhoc`, `archive`) |
| `last_execution_date` / `last_execution_context` | last query execution snapshot |

Useful queries:

```sql
-- Daily success/failure counts by rule
SELECT date(archived_at) AS d, parent_folder AS rule, status, COUNT(*) cnt
FROM `<project>.int_metabase_<env>.archiving_log`
WHERE object_type = 'card'
GROUP BY 1, 2, 3 ORDER BY 1 DESC, 4 DESC;

-- Cards that have repeatedly failed (worth investigating)
SELECT id, COUNT(*) failures, MAX(archived_at) last_attempt
FROM `<project>.int_metabase_<env>.archiving_log`
WHERE object_type = 'card' AND COALESCE(status, '') != 'success'
GROUP BY id HAVING failures > 1
ORDER BY failures DESC;
```

## 2. Permissions

`permissions` reconciles the Metabase collection permission graph (`/api/collection/graph`) with the YAML config. It's safe to run repeatedly — only differences are pushed.

### Algorithm

For each collection node in the YAML:

1. Apply its `permissions` block to the graph.
2. If `enforce_on_children: true`, walk every sub-collection (recursive `/api/collection/{id}/items?models=collection`) and apply the same permissions to all of them.
3. Process explicit `children:` overrides **after** step 2 — they win when both apply.

This means: parent broadcasts a default to its descendants, children explicitly listed in YAML can carve out exceptions.

The graph supports `"write"`, `"read"`, `"none"`. Since Metabase 0.56.13 the API omits any `(group, collection)` pair whose permission is `"none"` — the code treats an absent group as already `"none"` (no-op). Granting `"read"` or `"write"` to a group that's missing from the graph entirely raises `ValueError`.

### Config (`config/permissions/{staging,production}.yaml`)

```yaml
collections:
  root:
    id: "root"                 # Metabase's root pseudo-collection
    enforce_on_children: true  # default for everything below
    permissions:
      2: "write"               # group ids → permission level
      14: "read"
      1:  "none"
    children:
      internal:
        id: 608
        enforce_on_children: true
        permissions:
          14: "write"          # override: data team gets write on /internal
        children:
          general:
            id: 617
            permissions:
              14: "read"       # override the override: read on /internal/general
      external:
        id: 607
        ...
```

The group ids correspond to Metabase user-group ids (visible in the Metabase admin UI). Adding new ids to the config requires the group to exist in Metabase first.

### Dry-run

```bash
uv run python main.py permissions --dry-run
```

Logs every change that *would* be made (`Collection X, Group Y: old → new`) without calling `PUT /api/collection/graph`.

## Running locally

The job assumes:

- `ENV_SHORT_NAME` set to `dev`, `stg`, or `prod` (selects the YAML).
- `PROJECT_NAME` set to the GCP project (used for BQ and Secret Manager).
- ADC credentials with access to the project.
- Three secrets in Secret Manager: `metabase_host_<env>`, `metabase-<env>_oauth2_client_id`, `metabase-api-secret-<env>`.

```bash
ENV_SHORT_NAME=dev PROJECT_NAME=<project> uv run python main.py stats
```

Tests are pure-unit, no network: `uv run pytest`.
