{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions("venue", "academic_extended") %}

with
    base_aggregation as (
        select
            date_trunc(date(collective_booking_used_date), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            offerer_id,
            offerer_name,
            venue_region_name,
            venue_academy_name,
            venue_epci_code,
            venue_city_code,
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
            offerer_name,
            offerer_id,
            venue_region_name,
            venue_academy_name,
            venue_epci_code,
            venue_city_code
    ),

    all_dimensions as (
        {{
            generate_top_ranking_by_dimensions(
                base_cte="base_aggregation",
                dimensions=dimensions,
                entity_fields=["offerer_name", "offerer_id"],
                aggregated_metrics=[
                    {
                        "field": "total_booking_amount",
                        "alias": "total_booking_amount",
                    },
                    {
                        "field": "total_number_of_tickets",
                        "alias": "total_number_of_tickets",
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
    offerer_name,
    total_booking_amount,
    total_number_of_tickets,
    total_booking_amount_ranked
from all_dimensions
