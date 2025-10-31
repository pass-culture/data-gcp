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
        {{
            generate_top_ranking_by_dimensions(
                base_cte="base_aggregation",
                dimensions=dimensions,
                entity_fields=["collective_offer_format"],
                aggregated_metrics=[
                    {"field": "total_booking_amount", "alias": "total_booking_amount"},
                    {"field": "total_number_of_tickets", "alias": "total_number_of_tickets"}
                ],
                ranking_metric="total_booking_amount",
                top_n=5
            )
        }}
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
