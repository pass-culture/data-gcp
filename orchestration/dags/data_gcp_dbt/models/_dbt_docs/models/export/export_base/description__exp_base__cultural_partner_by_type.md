---
title: Cultural partners by venue type (export base)
description: Description of the `exp_base__cultural_partner_by_type` table, the base of the cultural partner by type exports.
---

{% docs description__exp_base__cultural_partner_by_type %}

# Table: Cultural partners by venue type (export base)

The `exp_base__cultural_partner_by_type` table holds the monthly active and cumulative activated cultural partners by venue type and venue geography, at the finest grain (municipality). Counts are not perturbed and not under statistical secret (partner lists are public).

{% enddocs %}

## Table description

A partner can match several types, and `tag_*` rows are labels (subsets of a type): never sum over `partner_type`. Use [exp_base__cultural_partner](description__exp_base__cultural_partner.md) for totals. Cells are additive over geography for a given type.

Read by [exp_vidoc_cultural_partner_by_type](../vidoc/description__exp_vidoc_cultural_partner__by_type.md) and `external_reporting__cultural_partner` (active individual partners by type).

{% docs table__exp_base__cultural_partner_by_type %}{% enddocs %}
