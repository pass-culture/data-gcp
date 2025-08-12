{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "execution_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "venue_region_name"},
] %}

{% for dim in dimensions %}
    {% if loop.first %}
        with
    {% endif %}
        cte_{{ dim.name }} as (
            select
                date_trunc(date(booking_used_date), month) as execution_date,
                date("{{ ds() }}") as update_date,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                venue_id,
                venue_name,
                offerer_name,
                sum(booking_intermediary_amount) as total_venue_booking_amount,
                row_number() over (
                    order by sum(booking_intermediary_amount) desc
                ) as total_venue_booking_amount_ranked
            from {{ ref("mrt_global__booking") }}
            where
                booking_is_used
                {% if is_incremental() %}
                    and date_trunc(date(booking_used_date), month)
                    = date_trunc(date("{{ ds() }}"), month)
                {% endif %}
            group by
                execution_date,
                update_date,
                dimension_name,
                dimension_value,
                venue_id

            qualify
                row_number() over (order by sum(booking_intermediary_amount) desc) <= 50
            order by
                execution_date,
                dimension_name,
                total_venue_booking_amount_ranked
        )
        {% if not loop.last %}, {% endif %}
{% endfor %}

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        execution_date,
        update_date,
        dimension_name,
        dimension_value,
        venue_id,
        venue_name,
        offerer_name,
        total_venue_booking_amount_ranked
    from cte_{{ dim.name }}
{% endfor %}
