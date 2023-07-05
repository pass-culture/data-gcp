SELECT 
    objective_name
    , objective_type
    , region_name
    , department_code
    , objective
FROM `{{ bigquery_raw_dataset }}.departmental_objectives`