-- noqa: disable=all
{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{#
    Share of students involved in at least one EAC action (ADAGE), rolled up
    from metrics_student__coverage (last ADAGE snapshot of each month).
#}
{% for dim in get_dimensions(none, "academic") %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "taux_participation_eac_jeunes" as kpi_name,
        coalesce(sum(total_engaged_students), 0) as numerator,
        coalesce(sum(total_eligible_students), 0) as denominator,
        safe_divide(
            coalesce(sum(total_engaged_students), 0),
            coalesce(sum(total_eligible_students), 0)
        ) as kpi
    from
        (
            select *, department_name as dep_name
            from {{ ref("metrics_student__coverage") }}
        )
    {% if is_incremental() %}
        where
            partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
    group by partition_month, dimension_name, dimension_value
{% endfor %}
