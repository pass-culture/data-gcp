The `external_reporting__beneficiary_booking` model provides the used bookings and amounts spent by the beneficiaries living in each territory (user geography), wherever they booked. Monthly flows by month of use, part of `external_reporting__individual`.

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
