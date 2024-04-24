SELECT 
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date,
    venue_id,
    venue_siret,
    venue_is_permanent,
    venue_type_code,
    venue_label_id,
    banner_url,
    venue_description,
    venue_audioDisabilityCompliant,
    venue_mentalDisabilityCompliant,
    venue_motorDisabilityCompliant,
    venue_visualDisabilityCompliant,
    venue_withdrawal_details
FROM `{{ bigquery_raw_dataset }}`.applicative_database_venue