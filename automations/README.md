# Automations

Internal tooling for scaffolding new `jobs/ml_jobs` / `jobs/etl_jobs` microservices
and keeping their `pyproject.toml` security constraints in sync. This documents
the automation itself — each *scaffolded* job gets its own README, generated
fresh by `uv init`, which is a separate thing.

## Layout

```
automations/
├── scripts/                       run via `uv run python automations/scripts/<name>.py`
│   ├── create_microservice.py     scaffolds a new job (see below)
│   ├── constraints.py             shared helpers: load/merge constraints, merge TOML tables
│   └── sync_constraint_dependencies.py   add missing security floors to existing jobs
└── configs/
    ├── microservice_types.yaml    per-type scaffold config (deps, python version, paths)
    ├── constraint_dependencies.yaml   shared org-wide security version floors
    └── templates/
        └── common_layout/         the file tree copied into every new job
```

## Creating a new microservice

```bash
MS_NAME=my_new_job make create_microservice_ml
MS_NAME=my_new_job make create_microservice_etl_internal
MS_NAME=my_new_job make create_microservice_etl_external
```

`ms_name` must be snake_case. These wrap the root Makefile's `create_microservice`
target, which calls `create_microservice.py --ms-name ... --ms-type ... --python-version $(MS_PYTHON)`.

What that script actually does, in order:

1. Look up the type's entry in `microservice_types.yaml` (template dir,
   destination path, dependencies, python version).
2. `shutil.copytree` the template dir (`templates/common_layout`) to the
   destination (e.g. `jobs/ml_jobs/my_new_job`).
3. `uv init --no-workspace -p <python_version>` to generate a real `pyproject.toml`.
4. Rewrite `requires-python` to `>=X.Y,<X.(Y+1)` (`uv init` alone only writes
   an open-ended `>=X.Y` floor).
5. `uv add <dependencies>` and `uv add --dev <dev_dependencies>`.
6. Merge in the shared security constraints (see below) and the template's
   `pyproject.toml.template` (ruff config + any `[tool.uv]` additions) via
   `constraints.merge_toml_tool_tables` — a real TOML merge, not a text
   append, since two files both declaring `[tool.uv]` is invalid TOML.
7. `uv sync`.
8. `git add -f data/.gitkeep` — `.gitignore` excludes `/data` wholesale, and
   git won't re-include a file inside an already-ignored parent directory, so
   this force-add is what keeps the empty `data/` folder trackable.

The Makefile's own `git add . && git commit -am ...` line is commented out —
uncomment it if you want the whole new job auto-committed, but note it stages
*everything* dirty in the repo, not just the new folder.

## `microservice_types.yaml`

```yaml
common:
  python_version: "3.13"        # keep in sync with MS_PYTHON in the Makefile
  dependencies: [...]           # every type gets these
  dev_dependencies: &dev_deps [...]

ml:
  template_dir: automations/configs/templates/common_layout
  destination: "jobs/ml_jobs/{ms_name}"
  ms_dependencies: [...]        # this type's additions on top of common.dependencies
  dev_dependencies: *dev_deps
```

`ms_dependencies` exists because YAML aliases only substitute a value
verbatim — `dependencies: *common_deps` can't be followed by more `- item`
lines to extend the list (that's invalid YAML). `create_microservice.py`'s
`load_scaffold_config` does the concatenation (`common.dependencies + ms_dependencies`)
in Python instead.

All three types currently share one `template_dir`
(`templates/common_layout`) since their file/folder structure is identical
today. Give a type its own template dir under `templates/` the day its
structure actually diverges — nothing else needs to change to support that.

## `templates/common_layout`

The standard shape every new job starts from:

```
main.py            # entrypoint: dispatches to one cli/<domain>.py module by name
cli/example.py      # thin layer — one Typer sub-app per "domain" (rename/add more)
src/                # business logic lives here, not in cli/
tests/
data/.gitkeep        # empty on purpose, see the git add -f step above
.gitignore
Makefile             # `make test`, `PYTHONPATH=. pytest tests`
pyproject.toml.template   # [tool.ruff] + any [tool.uv] additions, merged in at scaffold time
```

`main.py` discovers domains by globbing `cli/*.py` — add a new file there
(e.g. `cli/ingestion.py` with its own `app = typer.Typer()`) and it's
immediately usable as `main.py ingestion <command>`, no wiring needed. Only
the requested domain's module gets imported, so a job that only needs one
optional-dependency group installed doesn't pay for another domain's heavier
imports just to invoke the entrypoint.

## Security constraints (`constraint_dependencies.yaml` + `sync_constraint_dependencies.py`)

`constraint_dependencies.yaml` is a flat list of `{spec, reason}` entries —
version floors for known CVEs/GHSAs. Per uv's `constraint-dependencies`
semantics, these only take effect if the package is already part of a
project's resolved dependency graph (direct or transitive); they never force
an install.

New jobs get the current list automatically (step 6 above). For *existing*
jobs:

```bash
make sync_constraints_dry_run   # preview, writes nothing
make sync_constraints           # apply
```

Or target more narrowly:

```bash
uv run python automations/scripts/sync_constraint_dependencies.py --module jobs/ml_jobs/finance
uv run python automations/scripts/sync_constraint_dependencies.py --ml     # all of jobs/ml_jobs
uv run python automations/scripts/sync_constraint_dependencies.py --etl
```

Exactly one of `--all` / `--ml` / `--etl` / `--module` is required. It's
additive-only: a package a job's own `pyproject.toml` already constrains is
left untouched, whatever version it pins — only packages missing from that
list get the shared floor appended. Edits use `tomlkit` so existing
formatting/comments elsewhere in the file are untouched.

## Known caveats

- Running any of these scripts' `--help` currently crashes with
  `TypeError: Parameter.make_metavar() missing 1 required positional argument: 'ctx'`.
  That's a `click`/`typer` version-skew bug in the root project's `dev`
  dependency group (`typer~=0.13.1` vs. a newer `click`), unrelated to this
  tooling — every actual command still works, only `--help` rendering is
  affected.
- `jobs/etl_jobs/external/appsflyer` is still on `requirements.txt`/pip and
  has no `pyproject.toml` `[project]` section — none of this tooling touches
  it; it needs a manual migration to uv before it can participate in any of
  the above (constraint syncing, etc.).
