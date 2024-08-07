with venues as (
    select
        v.* except (venue_department_code),
        COALESCE(
            case
                when v.venue_postal_code = '97150' then '978'
                when SUBSTRING(v.venue_postal_code, 0, 2) = '97' then SUBSTRING(v.venue_postal_code, 0, 3)
                when SUBSTRING(v.venue_postal_code, 0, 2) = '98' then SUBSTRING(v.venue_postal_code, 0, 3)
                when SUBSTRING(v.venue_postal_code, 0, 3) in ('200', '201', '209', '205') then '2A'
                when SUBSTRING(v.venue_postal_code, 0, 3) in ('202', '206') then '2B'
                else SUBSTRING(v.venue_postal_code, 0, 2)
            end,
            v.venue_department_code
        ) AS venue_department_code,
        CASE 
            WHEN gp.banner_url IS NOT NULL THEN "offerer"
            WHEN gp.venue_id IS NOT NULL THEN "google"
            ELSE "default_category" END AS venue_image_source

    FROM {{ source('raw', 'applicative_database_venue') }} AS v
    LEFT JOIN {{ source('raw', 'applicative_database_google_places_info') }} AS gp ON v.venue_id = gp.venue_id
),

venues_with_geo_candidates as (
    select
        v.venue_id,
        v.venue_latitude,
        v.venue_longitude,
        gi.iris_internal_id,
        gi.region_name,
        gi.iris_shape
    from venues as v
        left join {{ ref('int_seed__geo_iris') }} as gi
            on v.venue_longitude between gi.min_longitude and gi.max_longitude
                and v.venue_latitude between gi.min_latitude and gi.max_latitude
),

venue_geo_iris as (
    SELECT *
    FROM venues_with_geo_candidates
    where ST_CONTAINS(
        iris_shape,
        ST_GEOGPOINT(venue_longitude, venue_latitude)
    ) or iris_shape is NULL
)

select
    venues.*,
    venue_geo_iris.iris_internal_id as venue_iris_internal_id,
    venue_geo_iris.region_name as venue_region_name
from venues
left join venue_geo_iris on venue_geo_iris.venue_id = venues.venue_id
