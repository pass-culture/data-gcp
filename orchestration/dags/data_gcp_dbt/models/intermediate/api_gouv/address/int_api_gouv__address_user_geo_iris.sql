{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "date_updated", "data_type": "datetime", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
) }}

with users_updated as (
    select
        user_id,
        user_address,
        user_postal_code,
        user_department_code,
        longitude,
        latitude,
        city_code,
        api_adresse_city,
        qpv_name,
        code_qpv,
        zrr,
        date_updated
    from {{ source("raw", "user_locations") }}
    {% if is_incremental() %}
        where date_updated >= DATE("{{ ds() }}") - 1
    {% endif %}
),

users_with_geo_candidates as (
    select
        u.*,
        gi.iris_internal_id as user_iris_internal_id,
        gi.region_name as user_region_name,
        gi.city_label as user_city,
        gi.epci_label as user_epci,
        gi.academy_name as user_academy_name,
        gi.density_label as user_density_label,
        gi.density_macro_level as user_macro_density_label,
        gi.iris_shape
    from users_updated as u
        left join {{ ref("int_seed__geo_iris") }} as gi
            on u.longitude between gi.min_longitude and gi.max_longitude
                and u.latitude between gi.min_latitude and gi.max_latitude
)

select * except (iris_shape)
from users_with_geo_candidates
where ST_CONTAINS(
        iris_shape,
        ST_GEOGPOINT(longitude, latitude)
    )
