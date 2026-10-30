Bookable (non template) collective offers by month, scholar year and **school** geography (location of the school the offer is made for), for the offers received by the schools of a territory.

Thin wrapper over [`int_kpi__offer_collective_bookable_institution`](#!/model/model.data_gcp_dbt.int_kpi__offer_collective_bookable_institution). An offer belongs to the scholar year of its action (stock beginning date). Its creation month is clamped into that scholar year: an offer created in June for an action in October is counted in September. Every month of the scholar year has a row for every geography seen in that scholar year.

Monthly columns are flows; `cumulative_*` columns are scholar year to date. Sum monthly columns over months, never `cumulative_*` columns.
