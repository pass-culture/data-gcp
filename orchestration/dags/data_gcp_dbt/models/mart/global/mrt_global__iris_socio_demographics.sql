with
    iris as (
        select iris_internal_id, iris_label, city_label, iris_shape, iris_centroid
        from {{ ref("int_seed__geo_iris") }}
    ),

    venues as (
        select
            venue_id,
            venue_city,
            venue_longitude,
            venue_latitude,
            st_geogpoint(venue_longitude, venue_latitude) as venue_point
        from {{ ref("int_global__venue") }}
        where venue_is_open_to_public = true
    ),

    candidates as (
        select
            i.iris_internal_id,
            v.venue_id,
            st_area(i.iris_shape) / 1000000 as iris_area_sq_km,
            st_distance(v.venue_point, i.iris_centroid) as distance_iris_venue
        from iris as i
        inner join venues as v on st_dwithin(v.venue_point, i.iris_centroid, 20000)  -- ← limite à 20 km
    ),

    iris_density as (
        select
            iris_internal_id,
            iris_area_sq_km,
            count(
                distinct case when distance_iris_venue < 20000 then venue_id end
            ) as total_venue_20_km,
            count(
                distinct case when distance_iris_venue < 5000 then venue_id end
            ) as total_venue_5_km
        from candidates
        group by iris_internal_id, iris_area_sq_km
    )

select
    gi.iris_code,
    gi.iris_internal_id,
    gi.iris_label,
    gi.city_code,
    gi.city_label,
    gi.territorial_authority_code,
    gi.epci_code,
    gi.epci_label,
    gi.department_code,
    gi.department_name,
    gi.region_name,
    gi.academy_name,
    gi.territorial_authority_label,
    gi.density_level,
    gi.density_label,
    gi.density_macro_level,
    gi.geo_code,
    gi.rural_city_type,
    id.iris_area_sq_km,
    id.total_venue_20_km,
    id.total_venue_5_km,
    sd.total_population,
    sd.population_15_years_or_more as total_population_15_years_or_more,
    sd.population_11_17_years as total_population_11_17_years,
    sd.population_18_24_years as total_population_18_24_years,
    sd.qt10_revenue as total_revenue_10_decile,
    sd.qt50_revenue as total_revenue_50_decile,
    sd.qt90_revenue as total_revenue_90_decile,
    safe_divide(sd.csp_1, sd.population_15_years_or_more) as pct_csp_1,
    safe_divide(sd.csp_2, sd.population_15_years_or_more) as pct_csp_2,
    safe_divide(sd.csp_3, sd.population_15_years_or_more) as pct_csp_3,
    safe_divide(sd.csp_4, sd.population_15_years_or_more) as pct_csp_4,
    safe_divide(sd.csp_5, sd.population_15_years_or_more) as pct_csp_5,
    safe_divide(sd.csp_6, sd.population_15_years_or_more) as pct_csp_6,
    safe_divide(sd.csp_7, sd.population_15_years_or_more) as pct_csp_7,
    safe_divide(sd.csp_8, sd.population_15_years_or_more) as pct_csp_8,
    safe_divide(sd.french_population, sd.total_population) as pct_french_population,
    safe_divide(sd.foreign_population, sd.total_population) as pct_foreign_population
from {{ ref("int_seed__geo_iris") }} as gi
left join iris_density as id on gi.iris_internal_id = id.iris_internal_id
left join
    {{ ref("int_seed__geo_iris_socio_demographics") }} as sd
    on gi.iris_code = sd.iris_code
