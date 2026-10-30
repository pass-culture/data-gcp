Statistical secrecy of cultural partners' revenue from **collective** bookings, by territory: whether a department, an EPCI or a municipality can be published. One row per `partition_month` × `rolling_window_months_count` (6 or 48 months) × `municipality_code`, for every municipality of the IRIS reference (most have no revenue). Revenue is the booking amount of used bookings over the window ending with the month, by venue territory.

The department and EPCI columns repeat on every municipality row of that department or EPCI: read them once per code (`select distinct`), never sum them. Always filter one `rolling_window_months_count`. The current month is partial. The table carries no indicator: it says whether a revenue figure for a territory may be published.

## Table description

Built by the `evaluate_statistical_secret` macro on `mrt_global__collective_booking`, from 2022-01 on.

For each territory and window:

1. **Primary secret** when the territory has fewer than 3 active partners (distinct venues with at least one used booking over the window), or when its top partner holds more than 85% of the revenue. A territory with no revenue is secret (`no_revenue`).
1. **Secondary secret**: when exactly one territory is under primary secret in its parent (a municipality in its EPCI, an EPCI in its department, a department in France), the non-secret territory with the smallest revenue in that parent is masked too (`secondary_secret`), so the secret figure cannot be recovered by subtraction from the parent's total.

`*_secret_reason_code` gives the reason (`no_revenue`, `low_partners`, `high_concentration`, `both`, `secondary_secret`, `none`); `*_secret_reason_label` is its French display label.

This rule differs from the `is_statistic_secret` flag of `metrics_booking__finance_individual`: at most 3 active partners in the city (venues with a bookable offer over 365 days, from `int_kpi__cultural_partner_activation`), with no concentration test and no secondary secret.
