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
            booking_is_used and offer_category_id != 'JEU'
            {% if is_incremental() %}
                and date_trunc(date(booking_used_date), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
    ),

    -- Dénombrement des variantes par item_id et par mois
    item_attribute_quantities as (
        select
            partition_month,
            item_id,
            offer_name,
            offer_category_id,
            offer_subcategory_id,
            sum(booking_quantity) as total_booking_quantity,
            sum(booking_intermediary_amount) as total_booking_amount
        from source_bookings
        group by
            partition_month,
            item_id,
            offer_name,
            offer_category_id,
            offer_subcategory_id
    ),

    -- Sélection des attributs canoniques (la version la plus réservée)
    item_attributes as (
        select
            partition_month,
            item_id,
            array_agg(
                struct(offer_name, offer_category_id, offer_subcategory_id)
                order by
                    total_booking_quantity desc,
                    total_booking_amount desc,
                    offer_name asc
                limit 1
            )[offset(0)] as attributes
        from item_attribute_quantities
        group by partition_month, item_id
    ),

    -- Agrégation géographique rattachée aux attributs canoniques uniques
    aggregated_by_item_geo as (
        select
            bo.partition_month,
            bo.item_id,
            ia.attributes.offer_category_id,
            ia.attributes.offer_subcategory_id,
            ia.attributes.offer_name,
            bo.venue_region_name,
            bo.venue_department_name,
            sum(bo.booking_intermediary_amount) as total_booking_amount,
            sum(bo.booking_quantity) as total_booking_quantity
        from source_bookings as bo
        inner join
            item_attributes as ia
            on bo.partition_month = ia.partition_month
            and bo.item_id = ia.item_id
        group by
            bo.partition_month,
            bo.item_id,
            ia.attributes.offer_category_id,
            ia.attributes.offer_subcategory_id,
            ia.attributes.offer_name,
            bo.venue_region_name,
            bo.venue_department_name
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
            additional_partition_by=["offer_category_id"],
            tie_breaker_field="item_id",
            top_n=50,
        )
    }}
