## Cumulative number of activated cultural partners

### Definition

A cultural partner (venue) is **activated** at a given month if it has had at least one **bookable** offer for at least one day between the start of the bookable offer history (**2022-06-10**) and the last day of the month. Creating an offer is not enough: the offer must have been bookable.

The count is cumulative: once activated, a partner stays counted in every following month, even if it is no longer active.

| Column                                                | Partners that have ever had...                                    |
| ----------------------------------------------------- | ----------------------------------------------------------------- |
| `total_cumulative_activated_partners_global`          | a bookable individual **or** collective offer                     |
| `total_cumulative_activated_partners_individual`      | a bookable individual offer                                       |
| `total_cumulative_activated_partners_collective`      | a bookable collective offer                                       |
| `total_cumulative_activated_partners_dual_part`       | both, concurrently or not                                         |
| `total_cumulative_activated_partners_individual_only` | a bookable individual offer and never a bookable collective offer |
| `total_cumulative_activated_partners_collective_only` | a bookable collective offer and never a bookable individual offer |

`global = individual_only + collective_only + dual_part`.

### Source table

[exp_vidoc_cultural_partner](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_cultural_partner/index.md). Same definitions as `external_reporting__cultural_partner` (`nombre_total_cumule_de_partenaires_actives_*`), which does not publish the global column.

### Calculation

```
SELECT
    partition_month,
    SUM(total_cumulative_activated_partners_global) AS activated_partners
FROM `cultural_partner`
GROUP BY partition_month
ORDER BY partition_month
```

### Available dimensions

Same as [active cultural partners](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/active_cultural_partners/index.md): `partition_month`, partner department and region.

### Temporal tracking

**Monthly**, cumulative. All months from January 2022 (zero before June 2022, start of the bookable offer history); the current month changes until it is closed.

### Notes and caveats

- "Since inception" means since **2022-06-10** (start of the bookable offer history), not since the launch of pass Culture: a venue whose offers were only bookable before that date is not counted.
- Venues are counted in their **current** department; venues that no longer exist in the applicative database are not counted.
- Same `partner_region_code = '-1'` handling and same data protection status as [active cultural partners](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/active_cultural_partners/index.md) (not perturbed, no secrecy).
