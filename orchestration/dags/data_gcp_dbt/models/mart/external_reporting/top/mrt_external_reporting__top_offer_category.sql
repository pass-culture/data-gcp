{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_quarter", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "venue_region_name"},
    {"name": "DEP", "value_expr": "venue_department_name"},
] %}

with
    base_aggregation as (
        select
            date_trunc(date(booking_used_date), quarter) as partition_quarter,
            timestamp("{{ ts() }}") as updated_at,
            item_id,
            offer_category_id,
            offer_subcategory_id,
            venue_region_name,
            venue_department_name,
            any_value(offer_name) as offer_name,
            sum(booking_intermediary_amount) as total_booking_amount,
            sum(booking_quantity) as total_booking_quantity
        from {{ ref("mrt_global__booking") }}
        where
            booking_is_used and offer_category_id <> "JEU"
            {% if is_incremental() %}
                and date_trunc(date(booking_used_date), quarter)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 quarter), quarter)
            {% endif %}
        group by
            partition_quarter,
            updated_at,
            item_id,
            offer_category_id,
            offer_subcategory_id,
            venue_region_name,
            venue_department_name
    ),

    all_dimensions as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_quarter,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                item_id,
                offer_category_id,
                offer_subcategory_id,
                offer_name,
                sum(total_booking_amount) as total_booking_amount,
                sum(total_booking_quantity) as total_booking_quantity,
                row_number() over (
                    partition by
                        partition_quarter,
                        offer_category_id
                        {% if not dim.name == "NAT" %}, {{ dim.value_expr }} {% endif %}
                    order by sum(total_booking_amount) desc
                ) as total_booking_amount_ranked
            from base_aggregation
            group by
                partition_quarter,
                updated_at,
                dimension_name,
                dimension_value,
                item_id,
                offer_category_id,
                offer_subcategory_id,
                offer_name
            qualify
                row_number() over (
                    partition by
                        partition_quarter,
                        offer_category_id
                        {% if not dim.name == "NAT" %}, {{ dim.value_expr }} {% endif %}
                    order by total_booking_amount desc
                )
                <= 50
        {% endfor %}
    )

select
    partition_quarter,
    updated_at,
    dimension_name,
    dimension_value,
    item_id,
    offer_category_id,
    offer_subcategory_id,
    offer_name,
    total_booking_amount,
    total_booking_quantity,
    total_booking_amount_ranked
from all_dimensions
