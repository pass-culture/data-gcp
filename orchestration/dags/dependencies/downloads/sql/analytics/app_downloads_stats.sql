select
    date(date) as app_download_date,
    'iOS' as app_provider,
    sum(units) as total_app_downloads
from `{{ bigquery_raw_dataset }}.apple_download_stats`

where product_type_identifier in ("1F")
group by app_download_date
union all
select
    date(date) as app_download_date,
    'Android' as app_provider,
    sum(daily_device_installs) as total_app_downloads
from `{{ bigquery_raw_dataset }}.google_download_stats`
group by app_download_date
