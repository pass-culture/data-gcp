{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions('venue', 'geo') %}

with
    base_aggregation as (
        select
            date_trunc(date(booking_used_date), month) as partition_month,
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
            booking_is_used
            {% if is_incremental() %}
                and date_trunc(date(booking_used_date), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
        group by
            partition_month,
            updated_at,
            item_id,
            offer_category_id,
            offer_subcategory_id,
            venue_region_name,
            venue_department_name
    ),

    all_dimensions as (
        {{
            generate_top_ranking_by_dimensions(
                base_cte="base_aggregation",
                dimensions=dimensions,
                entity_fields=[
                    "item_id",
                    "offer_category_id",
                    "offer_subcategory_id",
                    "offer_name",
                ],
                aggregated_metrics=[
                    {
                        "field": "total_booking_amount",
                        "alias": "total_booking_amount",
                    },
                    {
                        "field": "total_booking_quantity",
                        "alias": "total_booking_quantity",
                    },
                ],
                ranking_metric="total_booking_amount",
                top_n=50,
            )
        }}
    )

select
    partition_month,
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
