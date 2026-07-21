---
title: Metabase Card Canonicalization
description: Description of the `int_metabase__card_canonical` table.
---

{% docs description__int_metabase__card_canonical %}
One row per in-scope Metabase card, mapping each card to the canonical card and canonical
home dashboard of its metric so downstream retrieval can collapse duplicate copies to a
single card and route to its governed home. Cards are grouped by `metric_key` (normalized
title); within a group the canonical card is the one whose best in-scope home ranks highest
under the quality-first rule — certified, then doc-backed (`dashboard_documentation`), then
tier (`key_dashboard` > `thematique` > `chantier`), with 3-month views only as a tiebreak —
then the lowest card id. Also emits `card_quality`, a governance-first score for ranking
which cards the index should trust. Deterministic and tested; consumed as extra columns on
`int_metabase__asset_catalog`.
{% enddocs %}

## Table description

{% docs table__int_metabase__card_canonical %}{% enddocs %}
