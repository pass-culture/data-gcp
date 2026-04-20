# ETL Package Architecture Proposal

## Executive Summary

Add UV workspace configuration to existing structure with minimal changes:
- Add `pyproject.toml` files to existing folders (no restructuring)
- Keep all folder names unchanged
- Enable workspace-based dependency management
- Backward-compatible Airflow operator upgrades

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Min Python Version** | 3.10 | Supports all current DAGs, modern type hints |
| **Venv Strategy** | Isolated per job | No cross-job dependency conflicts |
| **Version Pinning** | Hybrid | Ranges for layers, locks for jobs |
| **CI Matrix** | Layered | Base: all versions / Jobs: specific version |
| **Structure** | Flat | No restructuring, just add pyproject.toml files |

---

## Current vs Target Structure

### Current (No changes to folders)

```
etl_jobs/
├── http_tools/
│   ├── __init__.py
│   ├── auth.py
│   ├── clients.py
│   └── ...
├── utils/
│   ├── __init__.py
│   └── gcp.py
├── connectors/
│   ├── __init__.py
│   ├── brevo/
│   └── titelive/
├── factories/
│   ├── __init__.py
│   ├── brevo.py
│   └── titelive.py
└── jobs/
    ├── titelive/
    │   ├── pyproject.toml    # Already exists
    │   └── ...
    └── brevo/
        └── ...
```

### Target (Add pyproject.toml files only)

```
etl_jobs/
├── pyproject.toml              # ADD: Root workspace
│
├── http_tools/                 # UNCHANGED
│   ├── pyproject.toml          # ADD
│   ├── __init__.py
│   ├── auth.py
│   ├── clients.py
│   └── ...
│
├── utils/                      # UNCHANGED
│   ├── pyproject.toml          # ADD
│   ├── __init__.py
│   └── gcp.py
│
├── connectors/                 # UNCHANGED
│   ├── pyproject.toml          # ADD
│   ├── __init__.py
│   ├── brevo/
│   └── titelive/
│
├── factories/                  # UNCHANGED
│   ├── pyproject.toml          # ADD
│   ├── __init__.py
│   ├── brevo.py
│   └── titelive.py
│
└── jobs/                       # UNCHANGED
    ├── titelive/
    │   ├── pyproject.toml      # EXISTS (update)
    │   ├── uv.lock             # ADD: job-specific lock
    │   └── ...
    └── brevo/
        ├── pyproject.toml
        ├── uv.lock
        └── ...
```

**Total changes: Add 5 new pyproject.toml files + 1 root pyproject.toml**

---

## Package Definitions

### Root Workspace: `etl_jobs/pyproject.toml`

```toml
[project]
name = "etl-workspace"
version = "0.1.0"
requires-python = ">=3.10"

[tool.uv.workspace]
members = [
    "http_tools",
    "utils",
    "connectors",
    "factories",
    "jobs/*",
]

[tool.uv]
dev-dependencies = [
    "pytest>=8.0",
    "ruff>=0.4",
    "mypy>=1.10",
]
```

### Layer 1a: `http_tools/pyproject.toml`

```toml
[project]
name = "http_tools"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "httpx>=0.27.0,<1.0",
    "requests>=2.31.0,<3.0",
    "backoff>=2.2.1,<3.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### Layer 1b: `utils/pyproject.toml`

```toml
[project]
name = "utils"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "google-cloud-secret-manager>=2.20.0,<3.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### Layer 2: `connectors/pyproject.toml`

```toml
[project]
name = "connectors"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "http_tools",
    "utils",
]

[project.optional-dependencies]
titelive = []
brevo = ["brevo-python>=1.1.0,<2.0"]

[tool.uv.sources]
http_tools = { workspace = true }
utils = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### Layer 3: `factories/pyproject.toml`

```toml
[project]
name = "factories"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "connectors",
]

[tool.uv.sources]
connectors = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### Layer 4: `jobs/titelive/pyproject.toml`

```toml
[project]
name = "titelive"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "factories",
    "google-cloud-bigquery>=3.0.0,<4.0",
    "google-cloud-storage>=2.16.0,<3.0",
    "pandas>=2.2.0,<3.0",
    "pandas-gbq>=0.19.0,<1.0",
    "pyarrow>=21.0.0,<23.0",
    "tqdm>=4.67.0,<5.0",
    "typer>=0.12.3,<1.0",
]

[tool.uv.sources]
factories = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

---

## Import Paths (Unchanged)

```python
# All existing imports continue to work
from http_tools.clients import SyncHttpClient
from connectors.titelive.client import TiteliveConnector
from factories.titelive import TiteliveFactory
from jobs.titelive.tasks import run_init
```

---

## Tests Structure (No src/ folder needed)

Jobs use flat layout - code at root, tests in `tests/` subfolder:

```
jobs/titelive/
├── pyproject.toml
├── uv.lock
├── __init__.py
├── main.py              # Code at root level
├── tasks.py
├── config.py
├── transform.py
├── load.py
└── tests/               # Tests subfolder
    ├── __init__.py
    ├── conftest.py      # Fixtures
    ├── test_tasks.py
    ├── test_transform.py
    └── test_load.py
```

**Run tests:**
```bash
# From workspace root
uv run pytest jobs/titelive/tests -v

# Or from job directory
cd jobs/titelive && uv run pytest tests -v
```

**Same for base layers:**
```
http_tools/
├── pyproject.toml
├── __init__.py
├── auth.py
├── clients.py
└── tests/
    ├── test_auth.py
    └── test_clients.py
```

---

## Dev/Test Dependencies (Separate from Production)

Test dependencies are kept separate using `[tool.uv]` dev-dependencies:

### Workspace-Level (Shared)

```toml
# etl_jobs/pyproject.toml (root)
[tool.uv]
dev-dependencies = [
    # Shared across all packages
    "pytest>=8.0",
    "pytest-cov>=4.0",
    "ruff>=0.4",
    "mypy>=1.10",
]
```

### Package-Level (Specific)

```toml
# jobs/titelive/pyproject.toml
[project]
name = "titelive"
dependencies = [
    "factories",
    "pandas>=2.2.0",
    # ... production deps only
]

[tool.uv]
dev-dependencies = [
    # Job-specific test deps (if needed)
    "pytest-asyncio>=0.23",
]
```

### Installation Commands

```bash
# Production only (CI deploy, GCE)
uv sync --package titelive

# Production + dev/test (CI testing, local dev)
uv sync --package titelive --dev

# Entire workspace with dev deps
uv sync --dev
```

### CI Example

```yaml
# Deploy job (production deps only)
deploy:
  steps:
    - run: uv sync --package titelive

# Test job (includes dev deps)
test:
  steps:
    - run: uv sync --package titelive --dev
    - run: uv run pytest jobs/titelive/tests -v --cov
```

### Alternative: Optional Extras

If you prefer explicit extras over UV's dev-dependencies:

```toml
[project.optional-dependencies]
test = [
    "pytest>=8.0",
    "pytest-cov>=4.0",
]
```

```bash
uv sync --extra test
```

---

## Version Pinning Strategy: Hybrid

### Layers 1-3: Version Ranges

```toml
dependencies = [
    "httpx>=0.27.0,<1.0",      # Allows patches, blocks breaking changes
]
```

### Layer 4 (Jobs): Locked Versions

```bash
# Each job has its own uv.lock (committed to git)
jobs/titelive/uv.lock
jobs/brevo/uv.lock
```

### Update Workflow

```bash
# Update single dependency
cd jobs/titelive
uv lock --upgrade-package pandas
git add uv.lock && git commit -m "chore: bump pandas"

# Update all dependencies
uv lock --upgrade
git add uv.lock && git commit -m "chore: update all deps"
```

---

## Dependency Graph

```
┌─────────────────────────────────────────────────────────────┐
│                   titelive (job)                            │
│  requires-python = ">=3.12"                                 │
│  uv.lock: deterministic                                     │
│  + pandas, typer, tqdm, google-cloud-*                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   factories                                 │
│  requires-python = ">=3.10"                                 │
│  version ranges (flexible)                                  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   connectors                                │
│  requires-python = ">=3.10"                                 │
│  version ranges (flexible)                                  │
└─────────────────────────────────────────────────────────────┘
                       │           │
                       ▼           ▼
┌────────────────────────────┐   ┌────────────────────────────┐
│       http_tools           │   │         utils              │
│  requires-python = ">=3.10"│   │  requires-python = ">=3.10"│
│  + httpx, requests, backoff│   │  + google-cloud-secret-mgr │
└────────────────────────────┘   └────────────────────────────┘
```

---

## CI Testing Matrix

### Base Layers

```yaml
strategy:
  matrix:
    python-version: ["3.10", "3.11", "3.12"]
    package: ["http_tools", "utils", "connectors", "factories"]

steps:
  - uses: astral-sh/setup-uv@v4
  - run: uv sync --package ${{ matrix.package }}
  - run: uv run pytest ${{ matrix.package }}/tests
```

### Jobs (specific Python version)

```yaml
jobs:
  titelive:
    steps:
      - uses: astral-sh/setup-uv@v4
        with:
          python-version: "3.12"
      - run: uv sync --package titelive
      - run: uv run pytest jobs/titelive/tests

  brevo:
    steps:
      - uses: astral-sh/setup-uv@v4
        with:
          python-version: "3.10"
      - run: uv sync --package brevo
      - run: uv run pytest jobs/brevo/tests
```

---

## Airflow Operator Upgrade

### Current Signature

```python
def __init__(
    self,
    instance_name: str,
    requirement_file: str = "requirements.txt",
    branch: str = "master",
    python_version: str = "3.10",
    base_dir: str = "data-gcp",
)
```

### Upgraded Signature (Backward Compatible)

```python
def __init__(
    self,
    instance_name: str,
    # === NEW PARAMETERS ===
    package: t.Optional[str] = None,           # uv sync --package {package}
    extras: t.Optional[t.List[str]] = None,    # uv sync --extra {extra}
    # === UNCHANGED ===
    requirement_file: str = "requirements.txt",
    branch: str = "master",
    environment: t.Dict[str, str] = {},
    python_version: str = "3.10",
    base_dir: str = "data-gcp",
)
```

### Install Command Priority

```python
def _build_install_command(self) -> str:
    # Priority 1: Workspace package
    if self.package:
        return f"uv sync --package {self.package}"

    # Priority 2: Extras
    if self.extras:
        extras_flags = " ".join(f"--extra {e}" for e in self.extras)
        return f"uv sync {extras_flags}"

    # Priority 3: Requirements file (legacy)
    if self.requirement_file:
        return f"""
            if [ -f "{self.requirement_file}" ]; then
                uv pip sync {self.requirement_file}
            else
                uv sync
            fi
        """

    # Priority 4: Fallback
    return "uv sync"
```

### Backward Compatibility

| Existing DAG | Behavior | Status |
|--------------|----------|--------|
| `requirement_file="jobs/x/requirements.txt"` | `uv pip sync` | ✅ Unchanged |
| No params specified | Uses `requirements.txt` | ✅ Unchanged |
| `package="titelive"` | `uv sync --package` | ✅ New |
| `extras=["brevo"]` | `uv sync --extra` | ✅ New |

### DAG Examples

**Legacy (no changes needed):**
```python
fetch_install_code = InstallDependenciesOperator(
    task_id="fetch_install_code",
    instance_name=GCE_INSTANCE,
    requirement_file="jobs/brevo/requirements.txt",
    python_version="3.10",
    base_dir=BASE_PATH,
)
```

**New workspace mode:**
```python
fetch_install_code = InstallDependenciesOperator(
    task_id="fetch_install_code",
    instance_name=GCE_INSTANCE,
    package="titelive",               # Matches folder name
    python_version="3.12",
    base_dir=BASE_PATH,
)
```

---

## Migration Roadmap

### Phase 1: Operator Upgrade (Non-Breaking)
- [ ] Add `package` and `extras` parameters
- [ ] Keep `requirement_file="requirements.txt"` default
- [ ] Deploy to Airflow
- [ ] All existing DAGs continue working

### Phase 2: Add pyproject.toml Files
- [ ] Create root `etl_jobs/pyproject.toml`
- [ ] Add `http_tools/pyproject.toml`
- [ ] Add `utils/pyproject.toml`
- [ ] Add `connectors/pyproject.toml`
- [ ] Add `factories/pyproject.toml`
- [ ] Test with `uv sync`

### Phase 3: Migrate Jobs
- [ ] Update `jobs/titelive/pyproject.toml` for workspace
- [ ] Generate `jobs/titelive/uv.lock`
- [ ] Update DAG to use `package="titelive"`
- [ ] Test in staging
- [ ] Repeat for other jobs

### Phase 4: CI/CD
- [ ] Add GitHub Actions for package testing
- [ ] Matrix: Python 3.10, 3.11, 3.12 for base layers
- [ ] Job-specific version testing

### Phase 5: Cleanup
- [ ] Remove `requirements.txt` files
- [ ] Add deprecation warning for `requirement_file`

---

## Local Development

```bash
cd data-gcp/jobs/etl_jobs

# Install entire workspace
uv sync

# Install specific job
uv sync --package titelive

# Run job
uv run python -m jobs.titelive.main run-incremental

# Update deps
cd jobs/titelive && uv lock --upgrade

# Run tests
uv run pytest http_tools/tests -v
uv run pytest jobs/titelive/tests -v
```

---

## Summary

| Aspect | Approach |
|--------|----------|
| **Structure** | Keep existing folders, add pyproject.toml only |
| **Folder names** | Unchanged (http_tools, utils, connectors, factories, jobs) |
| **Package names** | Match folder names exactly |
| **Layout** | Flat (no src/ folder), tests in `tests/` subfolder |
| **Imports** | Unchanged |
| **Python** | Min 3.10, job-specific can require higher |
| **Pinning** | Ranges for layers, locks for jobs |
| **Operator** | Backward-compatible with new `package` param |
