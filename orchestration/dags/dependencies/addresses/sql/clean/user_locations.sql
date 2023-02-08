SELECT * EXCEPT(id, irisCode, centroid, shape), iris_france.id AS iris_id
FROM `{{ bigquery_raw_dataset }}.user_locations`
LEFT JOIN `{{ bigquery_clean_dataset }}.iris_france` iris_france
ON 1 = 1
WHERE ST_CONTAINS(shape, ST_GEOGPOINT(longitude, latitude))