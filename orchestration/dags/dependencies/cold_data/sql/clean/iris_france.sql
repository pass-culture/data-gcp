SELECT 
    irisf.id, 
    irisf.iriscode,
    irisf.centroid,
    irisf.shape,
    embi.region_name,
    embi.department
FROM  `{{ bigquery_raw_dataset }}.iris_france` AS irisf
LEFT JOIN `{{ bigquery_raw_dataset }}.emboitements_iris` AS embi ON irisf.irisCode = embi.code_iris