{#
    The tops of generate_monthly_geo_top_rankings carry geo_value = region or
    department name only. This join adds the codes and names of the region and
    department, from the seed the venue location names come from (so names match
    by construction). Department rows also get their region; national rows get
    nulls. Columns: geo.region_code, geo.region_name, geo.department_code,
    geo.department_name.
#}
{% macro top_ranking_geo_join(relation_alias) %}
    left join
        (
            with
                regions as (
                    select region_name, max(region_code) as region_code
                    from {{ source("seed", "region_department") }}
                    group by region_name
                )

            select
                'region' as geo_level,
                region_name as geo_value,
                region_code,
                region_name,
                cast(null as string) as department_code,
                cast(null as string) as department_name
            from regions
            union all
            select
                'department' as geo_level,
                rd.dep_name as geo_value,
                regions.region_code,
                rd.region_name,
                rd.num_dep as department_code,
                rd.dep_name as department_name
            from {{ source("seed", "region_department") }} as rd
            left join regions on rd.region_name = regions.region_name
        ) as geo
        on {{ relation_alias }}.geo_level = geo.geo_level
        and {{ relation_alias }}.geo_value = geo.geo_value
{% endmacro %}
