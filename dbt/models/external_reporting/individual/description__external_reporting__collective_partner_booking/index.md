The `external_reporting__collective_partner_booking` model provides the collective bookings and amounts received by the cultural partners of each territory (venue geography). It is part of `external_reporting__individual` so that cultural partner pages read individual and collective figures from the same table.

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
