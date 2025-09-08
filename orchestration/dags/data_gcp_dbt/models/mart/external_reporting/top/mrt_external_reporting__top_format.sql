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
    {"name": "REG", "value_expr": "institution_region_name"},
    {"name": "ACAD", "value_expr": "institution_academy_name"},
] %}

with
    base_aggregation as (
        select
            date_trunc(date(collective_booking_used_date), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            collective_offer_format,
            institution_region_name,
            institution_academy_name,
            sum(booking_amount) as total_booking_amount,
            sum(collective_stock_number_of_tickets) as total_number_of_tickets
        from {{ ref("mrt_global__collective_booking") }}
        where
            is_used_collective_booking
            {% if is_incremental() %}
                and date_trunc(date(collective_booking_used_date), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
        group by
            partition_month,
            updated_at,
            collective_offer_format,
            institution_region_name,
            institution_academy_name
    ),

    all_dimensions as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                collective_offer_format,
                sum(total_booking_amount) as total_booking_amount,
                sum(total_number_of_tickets) as total_number_of_tickets,
                row_number() over (partition by partition_month
                        {% if not dim.name == "NAT" %}, {{ dim.value_expr }} {% endif %}
                    order by sum(total_booking_amount) desc
                ) as total_booking_amount_ranked
            from base_aggregation
            group by
                partition_month,
                updated_at,
                dimension_name,
                dimension_value,
                collective_offer_format
            qualify
                row_number() over (partition by partition_month
                        {% if not dim.name == "NAT" %}, {{ dim.value_expr }} {% endif %}
                    order by total_booking_amount desc
                )
                <= 5
        {% endfor %}
    )

select
    partition_month,
    updated_at,
    dimension_name,
    dimension_value,
    collective_offer_format,
    total_booking_amount,
    total_number_of_tickets,
    total_booking_amount_ranked
from all_dimensions
