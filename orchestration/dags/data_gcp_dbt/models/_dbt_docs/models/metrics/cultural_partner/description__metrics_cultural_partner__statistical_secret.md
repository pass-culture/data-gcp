---
title: Cultural Partner Statistical Secret
description: Description of the `metrics_cultural_partner__statistical_secret` table.
---

{% docs description__metrics_cultural_partner__statistical_secret %}
Statistical secret evaluation of cultural partner revenue, per period, booking type and geographic level (country, department, EPCI, municipality).

Periods are fixed and non-overlapping, `statistical_secret_period_months` months long (12 by default) and starting on `partition_month`: calendar years for individual bookings, school years (September to August) for collective bookings. Only completed periods are published. Fixed periods keep the secret status stable over time and prevent recovering monthly values by differencing overlapping windows.

Dashboards and exports must only display revenue of rows where `is_statistic_secret` is false, and must not re-aggregate rows: every published level is already computed.
{% enddocs %}

## Rules

The unit is the offerer (legal entity), not the venue: several venues of the same offerer count as one contributor.

**Primary secret** (INSEE rule for business statistics): a cell is secret when
1. fewer than 3 offerers contribute to its revenue, or
2. a single offerer generates 85% or more of its revenue.

Municipalities below `statistical_secret_min_municipality_population` inhabitants are also hidden (0 by default, i.e. disabled).

**Secondary secret**: a hidden cell must not be recoverable by subtracting published cells from a published parent. For each published parent, the residual "parent minus published cells it contains" must pass the primary rule; otherwise its smallest published children are hidden until it does. Steps run bottom-up:
1. municipalities within each EPCI,
2. within each department: EPCIs lying in that department only, and municipalities outside any EPCI (hiding an EPCI hides its municipalities),
3. departments within France (hiding a department hides everything below it).

Overseas collectivities (975, 977, 978, 986, 987, 988), which have no EPCI and very few partners, are grouped into a single department-level cell `COM` ("Collectivités d'outre-mer"). Their municipalities are still evaluated one by one.

EPCIs spanning several departments never count as covering a department or France. The INSEE pseudo-EPCI `ZZZZZZZZZ` is treated as "no EPCI".

**Remainder rows** (`*_remainder` levels) publish that residual, so that published children plus the remainder add up to the parent. They are covered by a data test asserting they are never secret.

## Thresholds

| Variable | Default |
|---|---|
| `statistical_secret_min_contributors` | 3 |
| `statistical_secret_max_dominance_share` | 0.85 |
| `statistical_secret_period_months` | 12 |
| `statistical_secret_min_municipality_population` | 0 |

## Limits

- The rule is evaluated on the levels of this table only. Publishing other breakdowns of the same revenue (category, region, combined booking types…) needs its own evaluation.
- Individual and collective revenue are evaluated separately: their sum must not be published unless both cells are publishable.
