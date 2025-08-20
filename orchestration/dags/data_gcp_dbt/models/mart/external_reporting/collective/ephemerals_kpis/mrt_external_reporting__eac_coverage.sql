{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "region_name"},
    {"name": "ACAD", "value_expr": "academy_name"},
] %}

{% set objects = [
    {"name": "jeunes", "attribute": "involved_students", "table_name": "adage_involved_student"},
    {"name": "eple", "attribute": "institutions", "table_name": "adage_involved_institution"},
] %}

with
    {% for obj in objects %}
        last_day_{{ obj.name }} as (
            select
                date_trunc(date, month) as partition_month,
                max(date) as last_date,
                max(cast(adage_id as int)) as last_adage_id
            from {{ ref(obj.table_name) }}
            where
                1 = 1
                {% if is_incremental() %}
                    and date_trunc(date, month)
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by 1
        ){% if not loop.last %},{% endif %}
        {% endfor %}

{% for obj in objects %}
{% if not loop.first %}
        union all
    {% endif %}
{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(involved.date, month) as partition_month,
        date("{{ ds() }}") as update_date,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "taux_participation_eac_{{ obj.name }}" as kpi_name,
        sum(involved.{{ obj.attribute }}) as numerator,
        sum(involved.total_{{ obj.attribute }}) as denominator,
        safe_divide(sum(involved.{{ obj.attribute }}), sum(involved.total_{{ obj.attribute }})) as kpi
    from {{ ref(obj.table_name) }} as involved
    -- take only last day for each month.
    inner join
        last_day_{{ obj.name }}
        on involved.date = last_day_{{ obj.name }}.last_date
        and date_trunc(involved.date, month) = last_day_{{ obj.name }}.partition_month
        and last_day_{{ obj.name }}.last_adage_id = cast(involved.adage_id as int)
    left join
        {{ source("seed", "region_department") }} as rd
        on involved.department_code = rd.num_dep
    where
        1 = 1 and {% if "{{ dim.name }}" == "NAT" %} involved.department_code = '-1'
        {% else %} not involved.department_code = "-1"
        {% endif %}
    group by partition_month, update_date, dimension_name, dimension_value, kpi_name
{% endfor %}
{% endfor %}
