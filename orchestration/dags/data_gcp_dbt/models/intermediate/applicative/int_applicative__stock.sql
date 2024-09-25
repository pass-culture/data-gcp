{% set target_name = var('ENV_SHORT_NAME') %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

with bookings_grouped_by_stock as (

    select
        stock_id,
        SUM(booking_quantity) as total_bookings,
        SUM(case when not booking_is_cancelled then booking_quantity else NULL end) as total_non_cancelled_bookings,
        SUM(case when booking_is_cancelled then booking_quantity end) as total_cancelled_bookings,
        SUM(case when booking_status = 'REIMBURSED' then booking_quantity end) as total_paid_bookings,
        COUNT(booking_id) as total_individual_bookings,
        COUNT(case when booking_is_cancelled then booking_id end) as total_cancelled_individual_bookings,
        COUNT(case when not booking_is_cancelled then booking_id end) as total_non_cancelled_individual_bookings,
        COUNT(case when booking_is_used then booking_id end) as total_used_individual_bookings,
        SUM(case when not booking_is_cancelled then booking_intermediary_amount end) as total_individual_theoretic_revenue,
        SUM(case when booking_is_used then booking_intermediary_amount end) as total_individual_real_revenue,
        SUM(case when booking_is_used and EXTRACT(year from booking_creation_date) = EXTRACT(year from CURRENT_DATE) then booking_intermediary_amount else NULL end) as total_individual_current_year_real_revenue,
        MIN(booking_creation_date) as first_individual_booking_date,
        MAX(booking_creation_date) as last_individual_booking_date,
        COUNT(case when booking_rank = 1 then booking_id end) as total_first_bookings
    from {{ ref('int_applicative__booking') }}
    group by stock_id
)

select
    s.stock_id,
    {{ target_schema }}.humanize_id(s.stock_id) as stock_humanized_id,
    s.stock_id_at_providers,
    s.stock_modified_at_last_provider_date,
    DATE(s.stock_modified_date) as stock_modified_date,
    s.stock_modified_date as stock_modified_at,
    COALESCE(s.stock_price, price_category.price) as stock_price,
    s.stock_quantity,
    s.stock_booking_limit_date,
    s.stock_last_provider_id,
    s.offer_id,
    s.stock_is_soft_deleted,
    s.stock_beginning_date,
    s.stock_creation_date,
    s.stock_fields_updated,
    s.price_category_id,
    s.stock_features,
    s.offerer_address_id,
    case when s.stock_quantity is NULL then NULL
        else GREATEST(s.stock_quantity - COALESCE(bs.total_non_cancelled_bookings, 0), 0)
    end as total_available_stock,
    bs.total_bookings,
    bs.total_non_cancelled_bookings,
    bs.total_cancelled_bookings,
    bs.total_paid_bookings,
    bs.total_individual_bookings,
    bs.total_cancelled_individual_bookings,
    bs.total_non_cancelled_individual_bookings,
    bs.total_used_individual_bookings,
    bs.total_individual_theoretic_revenue,
    total_individual_real_revenue,
    total_individual_current_year_real_revenue,
    bs.first_individual_booking_date,
    bs.last_individual_booking_date,
    bs.total_first_bookings,
    case when (
            (DATE(s.stock_booking_limit_date) > (DATE("{{ ds() }}") - 1) or s.stock_booking_limit_date is NULL)
            and (DATE(s.stock_beginning_date) > (DATE("{{ ds() }}") - 1) or s.stock_beginning_date is NULL)
            -- <> available_stock > 0 OR available_stock is null
            and (GREATEST(s.stock_quantity - COALESCE(bs.total_non_cancelled_bookings, 0), 0) > 0 or s.stock_quantity is NULL)
            and not s.stock_is_soft_deleted
        ) then TRUE
        else FALSE
    end as is_bookable,
    price_category.price_category_label_id,
    price_category_label.label as price_category_label,
    RANK() over (partition by s.offer_id order by s.stock_creation_date desc, s.stock_id desc) as stock_rk
from {{ source('raw','applicative_database_stock') }} as s
    left join bookings_grouped_by_stock as bs on bs.stock_id = s.stock_id
    left join {{ source('raw','applicative_database_price_category') }} as price_category on price_category.price_category_id = s.price_category_id
    left join {{ source('raw','applicative_database_price_category_label') }} as price_category_label on price_category.price_category_label_id = price_category_label.price_category_label_id
