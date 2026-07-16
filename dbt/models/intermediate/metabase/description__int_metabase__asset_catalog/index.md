One row per Metabase asset (dashboard or card) with its placement in the collection tree, the squad/tier/certified classification resolved by the metabase-governance `taxonomy` job, usage signals, and (for dashboards) the markdown / member-card context. The classified asset catalog used downstream for retrieval routing and exposure selection.

## Table description

| name                 | data_type | description                                                                                                                                                                           |
| -------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| asset_id             |           | Metabase id of the card or dashboard.                                                                                                                                                 |
| asset_kind           |           | Whether the row is a 'card' or a 'dashboard'.                                                                                                                                         |
| tier                 |           | Steering tier resolved from the ancestor collections.                                                                                                                                 |
| certified            |           | True when the certified ancestor collection (617) is in the chain.                                                                                                                    |
| in_scope             |           | True when the asset's root collection is in scope and the asset is neither archived nor personal. In-scope rows are never excluded.                                                   |
| dashboard_parameters |           | Dashboard filter widgets as an array of {name, slug, type}; `slug` is the URL key for deep-linking (`/dashboard/<id>?<slug>=<value>`). Empty array when the dashboard has no filters. |
