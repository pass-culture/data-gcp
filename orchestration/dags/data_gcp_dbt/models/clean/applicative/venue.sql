with
    venues as (
        select
            v.* except (venue_department_code),
            coalesce(
                case
                    when v.venue_postal_code = '97150'
                    then '978'
                    when substring(v.venue_postal_code, 0, 2) = '97'
                    then substring(v.venue_postal_code, 0, 3)
                    when substring(v.venue_postal_code, 0, 2) = '98'
                    then substring(v.venue_postal_code, 0, 3)
                    when
                        substring(v.venue_postal_code, 0, 3)
                        in ('200', '201', '209', '205')
                    then '2A'
                    when substring(v.venue_postal_code, 0, 3) in ('202', '206')
                    then '2B'
                    else substring(v.venue_postal_code, 0, 2)
                end,
                v.venue_department_code
            ) as venue_department_code,
            case
                when gp.banner_url is not null
                then "offerer"
                when gp.venue_id is not null
                then "google"
                else "default_category"
            end as venue_image_source

        from {{ source("raw", "applicative_database_venue") }} as v
        left join
            {{ source("raw", "applicative_database_google_places_info") }} as gp
            on v.venue_id = gp.venue_id
    )

select venues.*, vl.venue_iris_internal_id, vl.venue_region_name
from venues
left join {{ ref("int_geo__venue_location") }} vl on vl.venue_id = venues.venue_id
