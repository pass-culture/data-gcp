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
)

select
    venues.*,
    vl.venue_iris_internal_id,
    vl.venue_region_name
from venues
left join {{ ref('int_geo__venue_location')}} vl on vl.venue_id = venues.venue_id
