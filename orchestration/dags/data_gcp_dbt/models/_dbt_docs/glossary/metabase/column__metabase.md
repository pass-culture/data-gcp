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
{% docs column__metric_key %} Normalized card title (lowercased, deaccented) used to group duplicate cards of the same metric. {% enddocs %}
{% docs column__canonical_card_id %} The single card chosen to represent the metric (`metric_key`) group; equals `card_id` for the canonical card. Chosen by the quality-first rule: best home (certified > doc-backed > tier > views), then lowest card id. {% enddocs %}
{% docs column__canonical_dashboard_id %} The metric's canonical home dashboard, i.e. the best in-scope home of the `canonical_card_id`. Copies inherit this so all point to one home. Null when the canonical card is on no in-scope dashboard. {% enddocs %}
{% docs column__is_canonical %} True when this card is its metric group's `canonical_card_id`. {% enddocs %}
{% docs column__is_duplicate %} True when this card is a non-canonical copy of its metric (`card_id != canonical_card_id`). Downstream collapses these to the canonical card. {% enddocs %}
{% docs column__card_quality %} Governance-first quality score in [0, 1] for ranking cards: 0.45 certified (card or its canonical home) + 0.30 tier (max of card/home, normalized) + 0.15 canonical home doc-backed + 0.10 has an in-scope home. Views are not folded in (they stay a downstream tiebreak). {% enddocs %}
