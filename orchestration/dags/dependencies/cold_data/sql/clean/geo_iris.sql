SELECT 
    * except(iris_centroid, iris_shape),
    ST_GEOGPOINT(
        CAST(SPLIT(REPLACE(iris_centroid, 'POINT(', ''), ' ')[ORDINAL(1)] AS FLOAT64),
        CAST(SPLIT(REPLACE(REPLACE(iris_centroid, 'POINT(', ''), ')', ''), ' ')[ORDINAL(2)] AS FLOAT64)
    ) AS iris_centroid,
    ST_GEOGFROMTEXT(iris_shape) AS iris_shape,
    ST_BOUNDINGBOX(iris_shape).xmin min_longitude,
    ST_BOUNDINGBOX(iris_shape).xmax AS max_longitude,
    ST_BOUNDINGBOX(iris_shape).ymin AS min_latitude,
    ST_BOUNDINGBOX(iris_shape).ymax AS max_latitude
FROM `{{ bigquery_raw_dataset }}.geo_iris`