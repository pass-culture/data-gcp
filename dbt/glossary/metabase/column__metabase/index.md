**dashboard_id**: Metabase dashboard id.

**dashboard_markdown**: Concatenated markdown of the dashboard's text widgets, in layout order.

**dashboard_parameters**: Dashboard filter widgets as an array of {name, slug, type}; `slug` is the URL key for deep-linking (`/dashboard/<id>?<slug>=<value>`). Empty array when the dashboard has no filters.

**asset_id**: Metabase id of the card or dashboard.

**asset_kind**: Whether the row is a 'card' or a 'dashboard'.

**tier**: Steering tier resolved from the ancestor collections.

**certified**: True when the certified ancestor collection (617) is in the chain.

**in_scope**: True when the asset's root collection is in scope and the asset is neither archived nor personal. In-scope rows are never excluded.

**collection_name**: Name of the asset's collection (used as the export collection filter).

**selection_reason**: Why the asset was selected ('classified' or 'usage_safety_net').
