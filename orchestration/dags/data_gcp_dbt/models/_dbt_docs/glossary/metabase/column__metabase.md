---
description: Description of the `metabase` columns.
title: Metabase
---

{% docs column__dashboard_id %} Metabase dashboard id. {% enddocs %}
{% docs column__dashboard_markdown %} Concatenated markdown of the dashboard's text widgets, in layout order. {% enddocs %}
{% docs column__dashboard_parameters %} Dashboard filter widgets as an array of {name, slug, type}; `slug` is the URL key for deep-linking (`/dashboard/<id>?<slug>=<value>`). Empty array when the dashboard has no filters. {% enddocs %}
{% docs column__asset_id %} Metabase id of the card or dashboard. {% enddocs %}
{% docs column__asset_kind %} Whether the row is a 'card' or a 'dashboard'. {% enddocs %}
{% docs column__tier %} Steering tier resolved from the ancestor collections. {% enddocs %}
{% docs column__certified %} True when the certified ancestor collection (617) is in the chain. {% enddocs %}
{% docs column__in_scope %} True when the asset's root collection is in scope and the asset is neither archived nor personal. In-scope rows are never excluded. {% enddocs %}
{% docs column__collection_name %} Name of the asset's collection (used as the export collection filter). {% enddocs %}
{% docs column__selection_reason %} Why the asset was selected ('classified' or 'usage_safety_net'). {% enddocs %}
