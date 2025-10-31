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
    {"name": "REG", "value_expr": "venue_region_name"},
    {"name": "DEP", "value_expr": "venue_department_name"},
] %}

with
    base_aggregation as (
        select
            date_trunc(date(bo.booking_used_date), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            bo.item_id,
            bo.offer_category_id,
            bo.offer_subcategory_id,
            bo.venue_region_name,
            bo.venue_department_name,
            vt.venue_tag_name,
            any_value(bo.offer_name) as offer_name,
            sum(bo.booking_intermediary_amount) as total_booking_amount,
            sum(bo.booking_quantity) as total_booking_quantity
        from {{ ref("mrt_global__booking") }} as bo
        inner join
            {{ ref("mrt_global__venue_tag") }} as vt
            on bo.venue_id = vt.venue_id
            and vt.venue_tag_category_label
            = "Comptage partenaire label et appellation du MC"
        where
            bo.booking_is_used
            {% if is_incremental() %}
                and date_trunc(date(bo.booking_used_date), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
        group by
            partition_month,
            updated_at,
            bo.item_id,
            bo.offer_category_id,
            bo.offer_subcategory_id,
            bo.venue_region_name,
            bo.venue_department_name,
            vt.venue_tag_name
    ),

    all_dimensions as (
        {{
            generate_top_ranking_by_dimensions(
                base_cte="base_aggregation",
                dimensions=dimensions,
                entity_fields=["item_id", "offer_category_id", "offer_subcategory_id", "offer_name", "venue_tag_name"],
                aggregated_metrics=[
                    {"field": "total_booking_amount", "alias": "total_booking_amount"},
                    {"field": "total_booking_quantity", "alias": "total_booking_quantity"}
                ],
                ranking_metric="total_booking_amount",
                top_n=50
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
    venue_tag_name,
    total_booking_amount,
    total_booking_quantity,
    total_booking_amount_ranked
from all_dimensions
