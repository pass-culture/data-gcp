{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions(none, "academic") %}

with
    last_day_jeunes as (
        select
            date_trunc(date, month) as partition_month,
            max(date) as last_date,
            max(cast(adage_id as int)) as last_adage_id
        from {{ ref("adage_involved_student") }}
        where
            1 = 1
            {% if is_incremental() %}
                and date_trunc(date, month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
        group by 1
    )

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(involved.date, month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "taux_participation_eac_jeunes" as kpi_name,
        coalesce(sum(involved.involved_students), 0) as numerator,
        coalesce(sum(involved.total_involved_students), 0) as denominator,
        safe_divide(
            coalesce(sum(involved.involved_students), 0),
            coalesce(sum(involved.total_involved_students), 0)
        ) as kpi
    from {{ ref("adage_involved_student") }} as involved
    -- take only last day for each month.
    inner join
        last_day_jeunes
        on involved.date = last_day_jeunes.last_date
        and date_trunc(involved.date, month) = last_day_jeunes.partition_month
        and last_day_jeunes.last_adage_id = cast(involved.adage_id as int)
    left join
        {{ source("seed", "region_department") }} as rd
        on involved.department_code = rd.num_dep
    where
        1 = 1 and {% if "{{ dim.name }}" == "NAT" %} involved.department_code = '-1'
        {% else %} not involved.department_code = "-1"
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endfor %}
