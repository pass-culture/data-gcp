{% set geo_levels = [
    {"level": "national", "value_expr": "'NAT'", "group_expr": "'NAT'"},
    {
        "level": "region",
        "value_expr": "venue_region_name",
        "group_expr": "venue_region_name",
    },
    {
        "level": "department",
        "value_expr": "venue_department_name",
        "group_expr": "venue_department_name",
    },
] %}

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

{% for geo in geo_levels %}
    {% if not loop.first %}
        union all
    {% endif %}

    select
        partition_month,
        '{{ geo.level }}' as geo_level,
        cast({{ geo.value_expr }} as string) as geo_value,
        item_id,
        offer_category_id,
        offer_subcategory_id,
        offer_name,
        sum(total_booking_amount) as total_booking_amount,
        sum(total_booking_quantity) as total_booking_quantity,
        row_number() over (
            partition by partition_month, {{ geo.group_expr }}
            order by sum(total_booking_amount) desc
        ) as offer_rank
    from aggregated_by_item_geo
    where {{ geo.value_expr }} is not null
    group by
        partition_month,
        geo_level,
        geo_value,
        item_id,
        offer_category_id,
        offer_subcategory_id,
        offer_name
    qualify offer_rank <= 50
{% endfor %}
