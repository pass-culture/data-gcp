select date, 'apple' as provider, sum(units) as total_downloads
from `{{ bigquery_raw_dataset }}.apple_download_stats`

where product_type_identifier in ("1F")
group by date
union all
select date, 'google' as provider, sum(daily_device_installs) as total_downloads
from `{{ bigquery_raw_dataset }}.google_download_stats`
group by date
