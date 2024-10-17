{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(pre_hook="{{create_humanize_id_function()}}") }}

with
    bookings_grouped_by_stock as (

        select
            stock_id,
            sum(booking_quantity) as total_bookings,
            sum(
                case when not booking_is_cancelled then booking_quantity else null end
            ) as total_non_cancelled_bookings,
            sum(
                case when booking_is_cancelled then booking_quantity end
            ) as total_cancelled_bookings,
            sum(
                case when booking_status = 'REIMBURSED' then booking_quantity end
            ) as total_paid_bookings,
            count(booking_id) as total_individual_bookings,
            count(
                case when booking_is_cancelled then booking_id end
            ) as total_cancelled_individual_bookings,
            count(
                case when not booking_is_cancelled then booking_id end
            ) as total_non_cancelled_individual_bookings,
            count(
                case when booking_is_used then booking_id end
            ) as total_used_individual_bookings,
            sum(
                case when not booking_is_cancelled then booking_intermediary_amount end
            ) as total_individual_theoretic_revenue,
            sum(
                case when booking_is_used then booking_intermediary_amount end
            ) as total_individual_real_revenue,
            sum(
                case
                    when
                        booking_is_used
                        and extract(year from booking_creation_date)
                        = extract(year from current_date)
                    then booking_intermediary_amount
                    else null
                end
            ) as total_individual_current_year_real_revenue,
            min(booking_creation_date) as first_individual_booking_date,
            max(booking_creation_date) as last_individual_booking_date,
            count(
                case when booking_rank = 1 then booking_id end
            ) as total_first_bookings
        from {{ ref("int_applicative__booking") }}
        group by stock_id
    )

select
    s.stock_id,
    {{ target_schema }}.humanize_id(s.stock_id) as stock_humanized_id,
    s.stock_id_at_providers,
    s.stock_modified_at_last_provider_date,
    date(s.stock_modified_date) as stock_modified_date,
    s.stock_modified_date as stock_modified_at,
    coalesce(s.stock_price, price_category.price) as stock_price,
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
    case
        when s.stock_quantity is null
        then null
        else
            greatest(s.stock_quantity - coalesce(bs.total_non_cancelled_bookings, 0), 0)
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
    case
        when
            (
                (
                    date(s.stock_booking_limit_date) > (date("{{ ds() }}") - 1)
                    or s.stock_booking_limit_date is null
                )
                and (
                    date(s.stock_beginning_date) > (date("{{ ds() }}") - 1)
                    or s.stock_beginning_date is null
                )
                -- <> available_stock > 0 OR available_stock is null
                and (
                    greatest(
                        s.stock_quantity - coalesce(bs.total_non_cancelled_bookings, 0),
                        0
                    )
                    > 0
                    or s.stock_quantity is null
                )
                and not s.stock_is_soft_deleted
            )
        then true
        else false
    end as is_bookable,
    price_category.price_category_label_id,
    price_category_label.label as price_category_label,
    rank() over (
        partition by s.offer_id order by s.stock_creation_date desc, s.stock_id desc
    ) as stock_rk
from {{ source("raw", "applicative_database_stock") }} as s
left join bookings_grouped_by_stock as bs on bs.stock_id = s.stock_id
left join
    {{ source("raw", "applicative_database_price_category") }} as price_category
    on price_category.price_category_id = s.price_category_id
left join
    {{ source("raw", "applicative_database_price_category_label") }}
    as price_category_label
    on price_category.price_category_label_id
    = price_category_label.price_category_label_id
