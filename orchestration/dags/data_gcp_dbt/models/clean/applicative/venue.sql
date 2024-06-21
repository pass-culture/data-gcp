WITH venues AS (
    SELECT
        v.* EXCEPT(venue_department_code),
        COALESCE(CASE
            WHEN v.venue_postal_code = '97150' THEN '978'
            WHEN SUBSTRING(v.venue_postal_code, 0, 2) = '97' THEN SUBSTRING(v.venue_postal_code, 0, 3)
            WHEN SUBSTRING(v.venue_postal_code, 0, 2) = '98' THEN SUBSTRING(v.venue_postal_code, 0, 3)
            WHEN SUBSTRING(v.venue_postal_code, 0, 3) in ('200', '201', '209', '205') THEN '2A'
            WHEN SUBSTRING(v.venue_postal_code, 0, 3) in ('202', '206') THEN '2B'
            ELSE SUBSTRING(v.venue_postal_code, 0, 2)
            END,
            v.venue_department_code
        ) AS venue_department_code
    FROM {{ source('raw', 'applicative_database_venue') }} AS v
),
geo_candidates AS (
    SELECT
        v.*,
        gi.iris_internal_id,
        gi.region_name,
        gi.iris_shape
    FROM venues AS v
    LEFT JOIN {{ source('clean', 'geo_iris') }} AS gi
        ON v.venue_longitude BETWEEN gi.min_longitude AND gi.max_longitude
           AND v.venue_latitude BETWEEN gi.min_latitude AND gi.max_latitude
    )

SELECT
    gc.*,
    gc.iris_internal_id AS venue_iris_internal_id,
    gc.region_name AS venue_region_name
FROM geo_candidates gc
WHERE ST_CONTAINS(
        gc.iris_shape,
        ST_GEOGPOINT(gc.venue_longitude, gc.venue_latitude)
)
