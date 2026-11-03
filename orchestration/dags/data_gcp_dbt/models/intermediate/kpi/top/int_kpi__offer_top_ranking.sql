with
    source_bookings as (
        select
            item_id,
            offer_category_id,
            offer_subcategory_id,
            offer_name,
            venue_region_name,
            venue_department_name,
            booking_intermediary_amount,
            booking_quantity,
            date_trunc(date(booking_used_date), month) as partition_month
        from {{ ref("mrt_global__booking") }}
        where
            booking_is_used
            {% if is_incremental() %}
                and date_trunc(date(booking_used_date), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
    ),

    aggregated_by_item_geo as (
        select
            partition_month,
            item_id,
            offer_category_id,
            offer_subcategory_id,
            venue_region_name,
            venue_department_name,
            any_value(offer_name) as offer_name,
            sum(booking_intermediary_amount) as total_booking_amount,
            sum(booking_quantity) as total_booking_quantity
        from source_bookings
        group by
            partition_month,
            item_id,
            offer_category_id,
            offer_subcategory_id,
            venue_region_name,
            venue_department_name
    )

    {{
        generate_monthly_geo_top_rankings(
            base_cte="aggregated_by_item_geo",
            entity_fields=[
                "item_id",
                "offer_category_id",
                "offer_subcategory_id",
                "offer_name",
            ],
            aggregated_metrics=[
                {"field": "total_booking_amount", "alias": "total_booking_amount"},
                {
                    "field": "total_booking_quantity",
                    "alias": "total_booking_quantity",
                },
            ],
            ranking_metric="total_booking_amount",
            rank_column_name="offer_rank",
            top_n=50,
        )
    }}
