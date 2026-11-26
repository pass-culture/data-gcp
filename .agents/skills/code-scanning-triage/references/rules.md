# Rule playbook

How to triage the rules met in this repo. Each entry: what the rule really matches, how to tell a true positive (TP)
from a false positive (FP) here, and the expected action. Add an entry when you meet a new rule or a new pattern
(step 8 of `SKILL.md`).

Suppression formats are in `SKILL.md` (step 5). `<id>` below is the alert's `rule` value, copied exactly.

## Where does the input come from?

Most verdicts depend on who controls the value that reaches the sink.

| Source | Trust | Examples |
|---|---|---|
| Constants, files of the repo, config modules | trusted | SQL files, `QUERY_CONFIGS`, `extensions` lists |
| Env vars and CLI args set by the DAG code | trusted | `ENV_SHORT_NAME`, `sys.argv` built by the operator |
| Airflow params, `dag_run.conf` | semi-trusted (anyone who can trigger the DAG) | `params.branch`, dates |
| Tables produced by our pipelines | semi-trusted | BigQuery exports, Cloud SQL tables |
| Third-party API responses, user-generated content | untrusted | Qualtrics, Contentful, Brevo, offer descriptions |
| HTTP requests to our services | untrusted | FastAPI apps in `jobs/ml_jobs/*/app` |

A value from a trusted source is not an injection vector: FP. Semi-trusted is a TP only if the value is
interpolated into code (SQL, shell, Python) without validation. Untrusted is a TP unless it is validated or bound.

## Semgrep

### `python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query` and `python.lang.security.audit.formatted-sql-query.formatted-sql-query`

- Fire on any `.execute(<non-literal>)`, including DuckDB and BigQuery clients, and often both on the same line:
  handle them together (one comment can list both ids).
- They also fire on queries that are **already parameterized** (`conn.execute(query, params)`): FP.
- Identifiers (table names, DuckDB extensions, file paths, an attached database URL) cannot be bound parameters.
  When they come from the repo config: FP.
- Values (dates, ids, user strings) interpolated with f-strings: fix with bound parameters
  (`conn.execute(query, [value])` for DuckDB, `QueryJobConfig(query_parameters=[...])` for BigQuery).
- A whole query read from a SQL file of the repo and wrapped in `COPY (...) TO ...`: FP.

### `python.lang.security.audit.subprocess-shell-true.subprocess-shell-true`

- Fix by default: pass a list and drop `shell=True` (`subprocess.run(["gsutil", "-m", "cp", "-r", uri, "."], check=True)`).
  Build the list from the arguments instead of formatting a string; check every caller.
- Keep `shell=True` only if the command needs shell features (pipes, `&&`, globbing) and every interpolated part is trusted:
  FP, with the reason.
- These helpers (`docker.py`, `gcs_io.py`, `build_and_push_docker_image.py`) run docker, gcloud or gsutil:
  never run them to test. Unit-test the argument list with a mocked `subprocess.run`.

### `python.lang.security.audit.logging.logger-credential-leak.python-logger-credential-disclosure`

- Matches log messages that contain words such as `token`, `api_key`, `secret`, whatever is actually logged.
- The value is redacted (`_redact_token`, `_redact_headers`), truncated, or is not a secret (an audience, a client id): FP.
- The secret itself is logged: TP. Remove it from the log or redact it. Do not just reword the message to dodge the rule.

### `python.flask.security.xss.audit.direct-use-of-jinja2.direct-use-of-jinja2`

- An XSS rule: it matters only for HTML served to a browser.
- Templates that render SQL, SPARQL, shell scripts or docs (`docs/hooks.py`, `.rq.j2`, `.sh.j2`, materialized views): FP for XSS.
  If the rendered text is SQL or shell, check the template variables as for SQL injection above.

### `trailofbits.python.pickles-in-pytorch.pickles-in-pytorch`

- `torch.load(..., weights_only=True)`: mitigated, FP.
- `torch.load` without `weights_only=True`: fix by adding it; if the file holds full objects (graphs, models),
  save a `state_dict` or tensors instead, or keep it and justify that the file is produced by our own pipeline in our bucket.
- `torch.save` alone (writing our own artifact): FP.

### `trailofbits.python.automatic-memory-pinning.automatic-memory-pinning`

- Performance advice, not security (`DataLoader` without explicit `pin_memory`). FP for security; the owner may set
  `pin_memory` if the job trains on GPU.
- The rule only checks that `pin_memory=` is written in the call (not `False`). A wrapper that forwards `**kwargs`
  is fixed without behaviour change by `pin_memory=kwargs.pop("pin_memory", False)` (PyTorch's default):
  cheaper than a suppression whose id does not fit in 88 columns at method indent.

### `python.lang.security.insecure-hash-algorithms.insecure-hash-algorithm-sha1`

- Still fires with `usedforsecurity=False`.
- Checksum, change detection, cache key, or a hash required by an external API (e.g. data.gouv.fr stores sha1): FP.
  Add `usedforsecurity=False` if missing.
- Signatures, passwords, tokens: TP, use sha256 or better.
- Switching to sha256 changes the stored values: do it only if nothing compares them with old ones.

### `python.lang.security.audit.non-literal-import.non-literal-import`

- `importlib.import_module(f"cli.{domain}")` from `sys.argv` in CLI entry points. The DAG passes the argument: low risk,
  but the fix is cheap. Check the value against the modules of `cli/` (or a dict of allowed domains) before importing.
- `automations/configs/templates/common_layout/main.py` is the scaffolding template: fix it too, so new jobs inherit the fix.
- Already checked against an allowlist before the import (e.g. `artist/main.py`, `sys.argv[1] in DOMAINS`): mitigated,
  suppress. The rule still fires: only a literal import silences it, which would defeat lazy per-domain imports.

### `python.lang.security.audit.eval-detected.eval-detected`

- Fix: replace with `json.loads` (JSON text) or `ast.literal_eval` (Python literal). Look at real data first
  (fixtures, API docs) to pick the right one. Third-party API data: always a TP.

### `python.lang.security.audit.insecure-transport.requests.request-session-with-http.request-session-with-http`

- `session.mount("http://", adapter)` only configures retries for `http://` URLs. If every URL used is `https://`,
  remove the mount (fix); otherwise TP: switch the URL to https.

### `python.fastapi.security.wildcard-cors.wildcard-cors`

- `allow_origins=["*"]` on a FastAPI app. Needs the owner: which browser front-ends call it?
  Server-to-server only: FP, or remove the CORS middleware. Called from a web app: restrict to its origins.
  Never decide alone: list it as a question at the checkpoint.
- Clue: an app served only as a Vertex AI custom container (`aiplatform.Model.upload` in `cli/deploy_model.py`) is reached
  through `aiplatform.googleapis.com` with IAM, never directly by a browser: the middleware has no effect, propose removing it
  (done for `edito_semantic_search`, DE-4955).

### `bash.lang.security.ifs-tampering.ifs-tampering`

- `$(IFS=,; echo "${arr[*]}")` sets IFS inside a command substitution (a subshell): it does not leak, FP.
  Changing IFS at the top level of a script: TP, scope it in a subshell or use `printf`/`jq`.

### `package_managers.uv.uv-missing-dependency-cooldown.uv-missing-dependency-cooldown`

- Mechanical, fixed in bulk (`exclude-newer = "7 days"` under `[tool.uv]`, through `automations/`). Skip in triage.

## SonarCloud

The GitHub alert `rule` is `<language>:<key>` (e.g. `python:S6890`); the inline comment takes only the key (`S6890`).

### `python:S6890` (`pytz` with Python 3.9+)

- Fix: `zoneinfo.ZoneInfo("Europe/Paris")` instead of `pytz.timezone(...)`. `pytz` needs `localize()`, `zoneinfo` does not:
  rewrite `tz.localize(dt)` as `dt.replace(tzinfo=tz)`. Run the area tests.

### `githubactions:S8541`, `githubactions:S8544`, `docker:S8541`, `docker:S8544`, `docker:S6506`

- Mechanical, fixed in bulk (`uv sync --locked`/`--frozen` with `--no-build`, `curl --proto '=https'`). Skip in triage.

### `githubactions:S8233`

- Write permissions at workflow level: move them to the jobs that need them, set `contents: read` at workflow level.
