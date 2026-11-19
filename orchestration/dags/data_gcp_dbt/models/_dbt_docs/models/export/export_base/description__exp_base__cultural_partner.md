---
title: Cultural partners (export base)
description: Description of the `exp_base__cultural_partner` table, the base of the cultural partner exports.
---

{% docs description__exp_base__cultural_partner %}

# Table: Cultural partners (export base)

The `exp_base__cultural_partner` table holds the monthly active and cumulative activated cultural partners (venues), by channel and venue geography, at the finest grain (municipality). Every export of partner counts reads it.

{% enddocs %}

## Table description

Partner counts are **out of scope of the statistical secret**, by rule and in every tool. The secret (INSEE rule: at least 3 businesses, none above 85% of the value) protects the economic values of a business, such as revenue and amounts, not counts of establishments, and partner lists are public. Venues are not people, so cell key perturbation does not apply either. The secret applies to bookings and revenue measured on the venue geography.

A venue has one location, so cells are additive: every tool sums them.

- [exp_vidoc_cultural_partner](../vidoc/description__exp_vidoc_cultural_partner.md): summed to the department;
- external reporting: `external_reporting__cultural_partner` and the collective share of active partners (`external_reporting__eac_cultural_partner`) sum these cells at every level (NAT, REG, ACAD, DEP, EPCI, COM).

Same definitions as `metrics_cultural_partner__activation`. See [Statistical confidentiality](../../../references/statistical_confidentiality.md).

{% docs table__exp_base__cultural_partner %}{% enddocs %}
