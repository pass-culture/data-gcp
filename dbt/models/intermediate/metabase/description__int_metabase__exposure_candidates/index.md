High-quality subset of `int_metabase__asset_catalog` selected for dbt exposure generation: every in-scope, active, classified (tiered or certified) asset plus a usage-based safety net. Replaces the legacy top-10-collections rule consumed by the metabase-dbt export job.

## Table description

| name             | data_type | description                                                            |
| ---------------- | --------- | ---------------------------------------------------------------------- |
| asset_id         |           | Metabase id of the card or dashboard.                                  |
| collection_name  |           | Name of the asset's collection (used as the export collection filter). |
| selection_reason |           | Why the asset was selected ('classified' or 'usage_safety_net').       |
