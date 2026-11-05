---
title: Cultural partner metrics by type for vidoc export
description: Key indicators from the `exp_vidoc_cultural_partner_by_type` model.
---

{% docs description__exp_vidoc_cultural_partner__by_type %}

The `exp_vidoc_cultural_partner_by_type` model provides the monthly number of active and cumulatively activated cultural partners (venues), by partner type.
It is designed to be exported to the ministry for vidoc visualisation. It reads `metrics_cultural_partner__activation_by_type`; `total_active_partners_individual` is the same KPI as `external_reporting__cultural_partner` (`nombre_de_partenaire_actif_individuel_<type>`).

{% enddocs %}

## Table Description

Each row represents the cultural partner indicators for a month, a partner department (current venue location) and a partner type.

**Grain**: `partition_month`, `partner_region_code`, `partner_department_code`, `partner_type`.

> **Never sum this table over `partner_type`.** A partner can belong to several types (a festival is in both `musique_live` and `festival`, a venue tagged "Cinéma d'art et d'essai" is in `cinemas` and in its own type), `tag_*` rows are labels that are subsets of a type, and partners whose type is not listed are in no type. Use `exp_vidoc_cultural_partner` for totals. Summing over departments and regions for one type is correct.

- Types and labels come from the `get_partner_types` macro, as in external_reporting. The partner type is the current one, applied to every month.
- Same months and same `partner_region_code = '-1'` handling as `exp_vidoc_cultural_partner`.

{% docs table__exp_vidoc__cultural_partner_by_type %}{% enddocs %}
