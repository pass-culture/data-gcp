{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions(none, "academic_extended") %}

with
    created_collective_offers as (
        select
            venue_region_name as region_name,
            venue_academy_name as academy_name,
            venue_epci as epci_name,
            venue_city as city_name,
            date_trunc(date(collective_offer_creation_date), month) as partition_month,
            coalesce(count(distinct collective_offer_id), 0) as total_created_offer
        from {{ ref("int_global__collective_offer") }}
        group by
            venue_region_name,
            venue_academy_name,
            partition_month,
            venue_epci,
            venue_city
    ),

    ac_booked_collective_offers as (
        select
            venue_region_name as region_name,
            venue_academy_name as academy_name,
            venue_epci as epci_name,
            venue_city as city_name,
            date_trunc(
                date(collective_booking_creation_date), month
            ) as partition_month,
            coalesce(count(distinct collective_booking_id), 0) as total_booking,
            coalesce(sum(booking_amount), 0) as total_booking_amount
        from {{ ref("mrt_global__collective_booking") }}
        where collective_booking_status in ('CONFIRMED', 'USED', 'REIMBURSED')
        group by
            venue_region_name,
            venue_academy_name,
            partition_month,
            venue_epci,
            venue_city
    )

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'total_offres_collectives_creees' as kpi_name,
        sum(total_created_offer) as numerator,
        1 as denominator,
        sum(total_created_offer) as kpi
    from created_collective_offers
    {% if is_incremental() %}
        where
            partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value
    union all
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'total_reservations_collectives' as kpi_name,
        sum(total_booking) as numerator,
        1 as denominator,
        sum(total_booking) as kpi
    from ac_booked_collective_offers
    {% if is_incremental() %}
        where
            partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value
    union all
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'total_montant_engage' as kpi_name,
        sum(total_booking_amount) as numerator,
        1 as denominator,
        sum(total_booking_amount) as kpi
    from ac_booked_collective_offers
    {% if is_incremental() %}
        where
            partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value
{% endfor %}
