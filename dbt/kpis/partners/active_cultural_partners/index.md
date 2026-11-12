## Number of active cultural partners by channel

### Definition

A cultural partner is a **venue** (`venue_id`) that has created at least one individual or collective offer.

A partner is **active** in a month if it had at least one **bookable** offer (individual or collective) on at least one day of the **365 days up to the last day of the month**. Bookability comes from the daily bookable offer history (`int_history__bookable_venue`): an offer that was created but never bookable does not count.

Split by channel:

| Column                                  | Partners with, in the 365-day window...                                                           |
| --------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `total_active_partners_global`          | a bookable individual **or** collective offer                                                     |
| `total_active_partners_individual`      | a bookable individual offer                                                                       |
| `total_active_partners_collective`      | a bookable collective offer                                                                       |
| `total_active_partners_dual_part`       | a bookable individual offer **and** a bookable collective offer (not necessarily on the same day) |
| `total_active_partners_individual_only` | a bookable individual offer and no bookable collective offer                                      |
| `total_active_partners_collective_only` | a bookable collective offer and no bookable individual offer                                      |

`global = individual_only + collective_only + dual_part`, `individual = individual_only + dual_part`, `collective = collective_only + dual_part`.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_cultural_partner](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_cultural_partner/index.md). Same KPIs as `external_reporting__cultural_partner` (`nombre_total_de_partenaire_actif_*`).

### Calculation

Sum the columns over the desired dimensions. Each partner is counted once per row, so sums over departments and regions are exact.

```
SELECT
    partition_month,
    SUM(total_active_partners_global) AS active_partners,
    SUM(total_active_partners_individual_only) AS active_individual_only,
    SUM(total_active_partners_collective_only) AS active_collective_only,
    SUM(total_active_partners_dual_part) AS active_both
FROM `cultural_partner`
GROUP BY partition_month
ORDER BY partition_month
```

### Available dimensions

| Dimension  | Column                                               | Description                                     |
| ---------- | ---------------------------------------------------- | ----------------------------------------------- |
| Month      | `partition_month`                                    | Observation month (window ends on its last day) |
| Department | `partner_department_code`, `partner_department_name` | Department of the venue                         |
| Region     | `partner_region_code`, `partner_region_name`         | Region of the venue                             |

### Temporal tracking

**Monthly**, rolling 365-day window. All months from January 2022 are present, as in external_reporting. The bookable offer history starts on **2022-06-10**: months before June 2022 are zero, and until May 2023 the 365-day window is truncated (active = activated). Use months from **June 2023** for a full window. The current month is computed up to today and changes until it is closed.

### Notes and caveats

- The venue location is its **current** address, applied to all past months: a venue that moved is counted in its current department for the whole history.
- Overseas collectivities outside any region (Saint-Pierre-et-Miquelon, Saint-Martin, Nouvelle-Calédonie) and non-located venues (department `980`) have `partner_region_code = '-1'`: they are in the national total but in no region (about 70 active partners).
- Individual bookable offers are only counted for offerers whose current validation status is `VALIDATED`; no such filter applies to collective offers.
- Past months can be slightly restated when the monthly metrics are recomputed (current address, offerer validation).

### Data protection

Partner counts are **not perturbed** and **not under statistical secrecy**, as in external_reporting: lists of cultural partners are public.
