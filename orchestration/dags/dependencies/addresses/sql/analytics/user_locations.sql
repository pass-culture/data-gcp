SELECT * EXCEPT (user_address) 
FROM `{{ bigquery_clean_dataset }}.user_locations`
QUALIFY ROW_NUMBER() over (PARTITION BY user_id ORDER BY date_updated DESC) = 1