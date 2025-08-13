-- cannot order partition table -> To order by
-- execution_date,
-- dimension_name,
-- total_booking_amount_ranked
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
    {"name": "DEP", "value_expr": "venue_department_name"},
] %}

with
    base_aggregation as (
        select
            date_trunc(date(booking_used_date), month) as execution_date,
            date("{{ ds() }}") as update_date,
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
            booking_is_used
            {% if is_incremental() %}
                and date_trunc(date(booking_used_date), month)
                = date_trunc(date("{{ ds() }}"), month)
            {% endif %}
        group by
            execution_date,
            update_date,
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
                execution_date,
                update_date,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                item_id,
                offer_category_id,
                offer_subcategory_id,
                offer_name,
                total_booking_amount,
                total_booking_quantity,
                row_number() over (
                    order by total_booking_amount desc
                ) as total_booking_amount_ranked
            from base_aggregation
            qualify row_number() over (order by total_booking_amount desc) <= 50
        {% endfor %}
    )

select
    execution_date,
    update_date,
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
