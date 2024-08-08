SELECT 
    irisf.id, 
    irisf.iriscode,
    irisf.centroid,
    irisf.shape,
    n.region_name,
    n.department
FROM  {{ source("seed", "iris_france") }} AS irisf
LEFT JOIN  {{ source("seed", "iris_nesting") }} AS n ON irisf.irisCode = n.code_iris