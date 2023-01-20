SELECT * except(rnk) 
FROM (
    SELECT 
        *
        , ROW_NUMBER() OVER (PARTITION BY siren ORDER BY update_date DESC) as rnk
    FROM `{{ bigquery_clean_dataset }}.siren_data`
) inn
WHERE rnk = 1