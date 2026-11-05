with
    source_venue_tags as (
        select venue_id, venue_tag_name
        from {{ ref("mrt_global__venue_tag") }}
        where
            venue_tag_category_label = 'Comptage partenaire label et appellation du MC'
    ),

    source_bookings as (
        select
            venue_id,
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

    -- one name, category and subcategory per item and month (those of the most
    -- booked offers), otherwise an item whose offers differ on them would be split
    -- into several rows of the same top
    item_attribute_quantities as (
        select
            partition_month,
            item_id,
            offer_name,
            offer_category_id,
            offer_subcategory_id,
            sum(booking_quantity) as total_booking_quantity
        from source_bookings
        group by
            partition_month,
            item_id,
            offer_name,
            offer_category_id,
            offer_subcategory_id
    ),

    item_attributes as (
        select
            partition_month,
            item_id,
            array_agg(
                struct(offer_name, offer_category_id, offer_subcategory_id)
                order by
                    total_booking_quantity desc,
                    offer_name asc,
                    offer_category_id asc,
                    offer_subcategory_id asc
                limit 1
            )[offset(0)] as attributes
        from item_attribute_quantities
        group by partition_month, item_id
    ),

    joined_and_aggregated_by_offer_tag as (
        select
            bo.partition_month,
            bo.item_id,
            ia.attributes.offer_category_id,
            ia.attributes.offer_subcategory_id,
            bo.venue_region_name,
            bo.venue_department_name,
            vt.venue_tag_name,
            ia.attributes.offer_name,
            sum(bo.booking_intermediary_amount) as total_booking_amount,
            sum(bo.booking_quantity) as total_booking_quantity
        from source_bookings as bo
        inner join source_venue_tags as vt on bo.venue_id = vt.venue_id
        left join
            item_attributes as ia
            on bo.partition_month = ia.partition_month
            and bo.item_id = ia.item_id
        group by
            bo.partition_month,
            bo.item_id,
            ia.attributes.offer_category_id,
            ia.attributes.offer_subcategory_id,
            bo.venue_region_name,
            bo.venue_department_name,
            vt.venue_tag_name,
            ia.attributes.offer_name
    )

    {{
        generate_monthly_geo_top_rankings(
            base_cte="joined_and_aggregated_by_offer_tag",
            entity_fields=[
                "item_id",
                "offer_category_id",
                "offer_subcategory_id",
                "offer_name",
                "venue_tag_name",
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
