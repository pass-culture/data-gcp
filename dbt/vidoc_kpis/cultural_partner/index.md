## Number of active and activated cultural partners

### Definition

Tracks the monthly volume of cultural partners that are either currently active or have been cumulatively activated, aggregated by partner type, department, and region.

- **Activated partner (`activated_partners`)**: A partner who has created at least one offer (individual, collective, or both, depending on the selected scope) since **January 1st, 2022**.
- **Active partner (`active_partners`)**: A partner who had at least one bookable offer (`bookable_offer` status in history) within the **365 days preceding the selected observation month**.

### Source table

[exp_vidoc\_\_cultural_partner](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/models/export/vidoc/description__exp_vidoc__cultural_partner.md)

### Calculation

Sum the desired metric columns over your target dimensions (`partition_month`, `partner_type`, `partner_region_code`, `partner_department_code`).

``` ``sql SELECT partition_month, partner_region_name, partner_department_code, partner_type, SUM(total_active_partners_global) AS active_partners_global, SUM(total_cumulative_activated_partners_global) AS cumulative_activated_partners_global FROM ```exp_vidoc\_\_cultural_partner\` WHERE partition_month = '2026-01-01' GROUP BY partition_month, partner_region_name, partner_department_code, partner_type ORDER BY partner_region_name, partner_department_code, partner_type
