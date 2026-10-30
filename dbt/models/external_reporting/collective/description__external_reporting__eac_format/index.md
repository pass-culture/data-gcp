The `external_reporting__eac_format` model provides collective bookings, amounts and tickets split by offer format (institution geography), rolled up from `metrics_booking__collective_by_format`. KPI names are `total_reservations_format_<format>`, `total_montant_depense_format_<format>` and `total_tickets_generes_format_<format>`, monthly flows. An offer with several formats counts in each of them.

## Table Description

Each row represents a key indicator calculated for a specific month and geographic aggregation level.

| name            | data_type | description                                                                                    |
| --------------- | --------- | ---------------------------------------------------------------------------------------------- |
| partition_month |           | Indicates the first day of the month the KPI refers to.                                        |
| updated_at      |           | The actual date when the data refresh was executed.                                            |
| dimension_name  |           | The geographic agregation of the kpi. Can be National, Regional or Departemental.              |
| dimension_value |           | The name of the geographic agregation of the kpi, if it exists. (region name, department name) |
| kpi_name        |           | The name of the kpi.                                                                           |
| numerator       |           | The numerator of the computed kpi.                                                             |
| denominator     |           | The denominator of the computed kpi.                                                           |
| kpi             |           | The value of the kpi. Equals to numerator divided by denominator.                              |
