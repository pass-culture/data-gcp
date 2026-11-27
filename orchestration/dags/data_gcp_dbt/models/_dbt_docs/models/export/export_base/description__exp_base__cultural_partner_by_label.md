---
title: Cultural partners by label (export base)
description: Description of the `exp_base__cultural_partner_by_label` table, the base of the cultural partner by label exports.
---

{% docs description__exp_base__cultural_partner_by_label %}

# Table: Cultural partners by label (export base)

The `exp_base__cultural_partner_by_label` table holds the monthly active and cumulative activated cultural partners by Ministry of Culture label and venue geography, at the finest grain (municipality). Counts are out of scope of the statistical secret, which protects economic values of a business, not counts of establishments (see [exp_base__cultural_partner](description__exp_base__cultural_partner.md)).

{% enddocs %}

## Table Description

**Grain**: `partition_month`, `partner_region_name`, `partner_department_code`, `partner_epci_code`, `partner_municipality_code`, `partner_label`.

- `partner_label` is a venue tag of category 1 (LIR, Cinéma d'art et d'essai, SMAC, SCIN, MdF, CDN, ...), or `ALL` for the partners holding at least one label, counted once.
- **Never sum over `partner_label`**: a partner can hold several labels. Use the `ALL` rows for the number of labelled partners.
- Same activity rules, months and `partner_region_code = '-1'` handling as [exp_base__cultural_partner](description__exp_base__cultural_partner.md).

{% docs table__exp_base__cultural_partner_by_label %}{% enddocs %}
