with
    source_bookings as (
        select
            venue_id,
            venue_name,
            offerer_name,
            venue_region_name,
            venue_department_name,
            booking_intermediary_amount,
            date_trunc(date(booking_used_date), month) as partition_month
        from {{ ref("mrt_global__booking") }}
        where
            booking_is_used
            {% if is_incremental() %}
                and date_trunc(date(booking_used_date), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
    ),

    aggregated_by_venue as (
        select
            partition_month,
            venue_id,
            venue_name,
            offerer_name,
            venue_region_name,
            venue_department_name,
            sum(booking_intermediary_amount) as total_booking_amount
        from source_bookings
        group by
            partition_month,
            venue_id,
            venue_name,
            offerer_name,
            venue_region_name,
            venue_department_name
    )

    {{
        generate_monthly_geo_top_rankings(
            base_cte="aggregated_by_venue",
            entity_fields=["venue_id", "venue_name", "offerer_name"],
            aggregated_metrics=[
                {"field": "total_booking_amount", "alias": "total_booking_amount"}
            ],
            ranking_metric="total_booking_amount",
            rank_column_name="venue_rank",
            top_n=50,
        )
    }}
