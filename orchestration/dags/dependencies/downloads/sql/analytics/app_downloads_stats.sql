SELECT 
    date, 
    'apple' as provider, 
    sum(units) as total_downloads
FROM `{{ bigquery_raw_dataset }}.apple_download_stats` 

WHERE product_type_identifier in ("1F") 
GROUP BY date
UNION ALL
SELECT 
    date, 
    'google' as provider, 
    sum(daily_device_installs) as total_downloads
FROM `{{ bigquery_raw_dataset }}.google_download_stats` 
GROUP BY date