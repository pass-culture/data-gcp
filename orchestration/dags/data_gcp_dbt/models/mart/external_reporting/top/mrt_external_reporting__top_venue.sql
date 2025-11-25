{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions("venue", "geo") %}

with
    base_aggregation as (
        select
            date_trunc(date(booking_used_date), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            venue_id,
            venue_name,
            offerer_name,
            venue_region_name,
            venue_department_name,
            sum(booking_intermediary_amount) as total_venue_booking_amount
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
            venue_id,
            venue_name,
            offerer_name,
            venue_region_name,
            venue_department_name
    ),

    all_dimensions as (
        {{
            generate_top_ranking_by_dimensions(
                base_cte="base_aggregation",
                dimensions=dimensions,
                entity_fields=["venue_id", "venue_name", "offerer_name"],
                aggregated_metrics=[
                    {
                        "field": "total_venue_booking_amount",
                        "alias": "total_venue_booking_amount",
                    }
                ],
                ranking_metric="total_venue_booking_amount",
                top_n=50,
            )
        }}
    )

select
    partition_month,
    updated_at,
    dimension_name,
    dimension_value,
    venue_id,
    venue_name,
    offerer_name,
    total_venue_booking_amount_ranked
from all_dimensions
