KPI table of collective bookings by month and **venue** geography (location of the cultural partner). The same KPIs by school geography are in [`int_kpi__booking_collective_institution`](#!/model/model.data_gcp_dbt.int_kpi__booking_collective_institution).

Each row represents a `partition_month` / `scholar_year` / venue geography combination. Every month of the scholar year has a row for every geography seen in that scholar year, so `cumulative_*` columns can be read on any month.

The model is built from [`mrt_global__collective_booking`](#!/model/model.data_gcp_dbt.mrt_global__collective_booking) and keeps non-cancelled bookings. Each booking is counted on three dates, each clamped into its scholar year (a booking created in June for the next scholar year is counted in September):

- creation: `total_prereservations` (all non-cancelled) and `total_collective_bookings` (confirmed today);
- confirmation by the school head: `total_confirmations`;
- action (stock beginning date): `total_actions`.

Monthly columns are flows; `cumulative_*` columns are scholar year to date. Sum monthly columns over months, never `cumulative_*` columns.
