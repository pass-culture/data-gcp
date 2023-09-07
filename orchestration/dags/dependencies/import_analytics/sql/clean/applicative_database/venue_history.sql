SELECT 
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date,
    venue_id,
    venue_siret,
    venue_is_permanent,
    venue_type_code,
    venue_label_id,
    banner_url
FROM `{{ bigquery_clean_dataset }}`.applicative_database_venue