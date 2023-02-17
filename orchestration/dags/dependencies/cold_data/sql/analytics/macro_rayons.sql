SELECT 
    rayon
    , TRIM(macro_rayon) as macro_rayon
FROM `{{ bigquery_raw_dataset }}.macro_rayons`