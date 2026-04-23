# Metabase Migration Tool - Complete Analysis Index

This directory contains comprehensive documentation for the Metabase migration tool project located at:
`/Users/espassculture/Development/git-repos/passculture/data-gcp/jobs/etl_jobs/external/metabase`

## 📚 Documentation Files

### 1. **metabase_project_analysis.md** (Comprehensive - 770 lines)
   
   **Best for:** Deep understanding of the entire system
   
   Sections:
   - Project overview & purpose
   - **Section 1:** Complete Justfile breakdown (all 19 recipes)
   - **Section 2:** main.py CLI structure (6-phase execution flow, all helper functions)
   - **Section 3:** Docker directory (docker-compose.yml, docker-setup.sh 6 phases)
   - **Section 4:** Project structure (directory layout, modern vs legacy modules)
   - **Section 5:** Configuration files (pyproject.toml, config.py environment variables)
   - **Section 6:** Key modules deep-dive:
     - api/client.py (MetabaseClient with retry logic)
     - api/models.py (Pydantic models for Card, Table, Field, etc.)
     - discovery/bigquery.py (card discovery, field mapping)
     - migration/card.py (migration logic for both card types)
     - migration/field_walker.py (safe MBQL tree walker)
   - **Section 7:** Data files (mappings.json structure)
   - **Section 8:** Testing structure
   - **Section 9:** Workflow examples (local & production)
   - **Section 10:** Key insights, strengths, limitations

### 2. **metabase_architecture.txt** (Visual - ASCII Diagrams)
   
   **Best for:** Visual understanding of flow and relationships
   
   Diagrams:
   - Main execution flow (6 phases with detailed boxes and arrows)
   - Card migration dispatch logic (native SQL vs query builder)
   - Field walker MBQL construct recognition (field refs, FK refs, source-table)
   - Docker environment setup (3 containers, bootstrap phases)

### 3. **QUICK_REFERENCE.md** (Quick Lookup - 1 page)
   
   **Best for:** Quick answers while working
   
   Sections:
   - What the tool does (2 card types supported)
   - Command line usage (local vs production)
   - Docker testing workflow (all 6 commands)
   - Key files table
   - 6-phase execution summary
   - Authentication modes
   - What gets updated (native vs query builder)
   - Safety features
   - Configuration quick reference
   - Testing commands
   - Important notes
   - Troubleshooting guide
   - Justfile recipes
   - Key concepts explained

## 🎯 How to Use These Docs

### If you need to...

**Understand the overall architecture:**
→ Start with `metabase_architecture.txt` for diagrams, then read Section 1-3 of `metabase_project_analysis.md`

**Understand how the CLI works:**
→ Read `QUICK_REFERENCE.md` "Command Line Usage", then Section 2 of `metabase_project_analysis.md`

**Understand the code modules:**
→ Read Section 6 of `metabase_project_analysis.md`

**Run local tests:**
→ Follow `QUICK_REFERENCE.md` "Docker Testing Workflow"

**Deploy to production:**
→ Read `QUICK_REFERENCE.md` "Authentication Modes" (Production) and Section 2 of `metabase_project_analysis.md`

**Debug a migration:**
→ Use `QUICK_REFERENCE.md` "Troubleshooting" section

**Understand the docker-test-migrate recipe:**
→ Look at `metabase_architecture.txt` "Docker Environment Setup" and Section 3 of `metabase_project_analysis.md`

**Add a new feature:**
→ Read Section 6 of `metabase_project_analysis.md` for module descriptions and understand the 6-phase flow from Section 2

## 🔑 Key Takeaways

### What the tool does:
Automatically updates Metabase cards (questions/dashboards) when BigQuery tables are renamed or columns change names.

### Architecture:
- **CLI:** Typer framework
- **Execution:** 6 phases (load mappings → connect → discover → map fields → migrate → log)
- **Card Types:** Native SQL (via regex) + Query Builder (via tree walker)
- **Safety:** Tree walker avoids regex-on-JSON hazards
- **Auth:** GCP OAuth2 (production) + direct auth (local)

### Key Files:
- `main.py` - CLI entry point
- `api/client.py` - HTTP client with retry logic
- `migration/card.py` - Migration dispatcher
- `migration/field_walker.py` - Safe MBQL tree walker
- `discovery/bigquery.py` - Card discovery
- `docker-compose.yml` - 3-container local environment
- `scripts/docker-setup.sh` - Bootstrap script
- `justfile` - 19 development recipes

### Testing:
Local Docker environment requires NO GCP credentials:
```bash
just docker-up              # Start containers
just docker-setup           # Bootstrap
just docker-prepare-migration  # Rename table/column
just docker-test-migrate    # Run migration --dry-run
```

## 📊 Statistics

| Metric | Value |
|--------|-------|
| Total recipes in justfile | 19 |
| Execution phases in main.py | 6 |
| Bootstrap phases in docker-setup.sh | 6 |
| Docker containers | 3 |
| Main Python modules | 5 (api, discovery, migration, config, main) |
| Pydantic models | 8 |
| Key strengths documented | 10 |

## 🏗️ Project Structure

```
metabase/
├── api/                    # HTTP client + data models
│   ├── client.py          # MetabaseClient with retry
│   └── models.py          # Pydantic models
├── discovery/             # BigQuery integration
│   └── bigquery.py        # Card discovery + field mapping
├── migration/             # Core migration logic
│   ├── card.py            # Main dispatcher
│   └── field_walker.py    # Safe MBQL tree walker
├── scripts/               # Utility scripts
│   └── docker-setup.sh    # Bootstrap Metabase
├── tests/                 # Test suite
├── data/                  # Column mapping JSON
├── docker/                # Docker configuration
├── docs/                  # Additional documentation
├── main.py               # CLI entry point
├── config.py             # Configuration management
├── pyproject.toml        # Project metadata
├── justfile              # Development tasks
└── README.md             # Project README (in French)
```

## 🚀 Quick Start

### Local Testing (no GCP needed):
```bash
just docker-up
just docker-setup
just docker-prepare-migration
just docker-test-migrate
just docker-reset
```

### Production Deployment:
```bash
export PROJECT_NAME="passculture-prod"
export ENV_SHORT_NAME="prod"
python main.py migrate \
  --legacy-table-name old_user_stats \
  --new-table-name enriched_user_data \
  --legacy-schema-name analytics \
  --new-schema-name analytics \
  --dry-run  # Preview first!
```

## 📝 Notes

- All documentation is READ-ONLY (created for analysis purposes)
- Located in `/tmp/` for reference
- Based on actual code analysis (no speculation)
- Includes both high-level architecture and code-level details
- Covers both local testing and production deployment

## 🔗 Related Concepts

**MBQL:** Metabase's internal query language for UI-built questions
- Field references: `["field", 42, null]`
- FK references: `["fk->", [...], [...]]`
- Source table: `{"source-table": 42}`

**Field Mapping:** How columns are renamed when tables are renamed
- Old column name → New column name (via mappings.json)
- Old field ID → New field ID (via API query)

**Card Discovery:** Finding which cards reference a table
- Query: `card_dependency` table in BigQuery
- Join: With `activity` table for usage metrics
- Sort: By highest usage first

---

**Generated:** Analysis completed for Metabase migration tool
**Source:** `/Users/espassculture/Development/git-repos/passculture/data-gcp/jobs/etl_jobs/external/metabase`
**Documentation:** 3 markdown/text files with 770+ lines of detailed analysis
