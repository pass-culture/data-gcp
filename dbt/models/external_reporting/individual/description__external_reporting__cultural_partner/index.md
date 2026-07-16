The `external_reporting__cultural_partner` model provides aggregated key indicators related to pass Culture cultural partners. It is designed for automated export, especially in Excel format.

## Table Description

Each row represents a key indicator calculated for a specific month and geographic aggregation level. A lexique details the meaning of each kpi_name here: https://docs.google.com/spreadsheets/d/13Epof9MJbtN7srmDIld3-7QN0Hq6WpX7/edit?usp=sharing&ouid=113033507472186952579&rtpof=true&sd=true

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
