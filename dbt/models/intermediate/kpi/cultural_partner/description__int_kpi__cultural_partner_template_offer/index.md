KPI table of cultural partners (venues) with at least one collective template offer ("offre vitrine"), by month and **partner** geography.

Each row represents a `partition_month` / partner geography combination. Every geography has a row for every month from its first template offer to the current month, so `total_cumulative_partners_with_template_offer` can be read on any month. A partner is counted on the month of its first template offer creation.

`total_new_partners_with_template_offer` is a monthly flow (sum it over months); `total_cumulative_partners_with_template_offer` is a stock (never sum it over months). Both are additive across geographies, a venue having one location.

The model is built from [`mrt_global__cultural_partner`](#!/model/model.data_gcp_dbt.mrt_global__cultural_partner) and [`int_global__collective_offer`](#!/model/model.data_gcp_dbt.int_global__collective_offer).
