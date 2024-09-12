{{
    config(
        tags = 'weekly',
        labels = {'schedule': 'weekly'},
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "execution_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns"
        )
) }}

with favorites as (
    select distinct
        favorite.userid as user_id,
        offerid as offer_id,
        offer.offer_name,
        offer.offer_subcategory_id as subcategory,
        (
            select count(*)
            from
                {{ ref('mrt_global__booking') }}
            where
                offer_subcategory_id = offer.offer_subcategory_id
                and user_id = favorite.userid
        ) as user_bookings_for_this_subcat
    from
        {{ source('raw', 'applicative_database_favorite') }} as favorite
        left join
            {{ ref('mrt_global__booking') }}
                as booking
            on favorite.userid = booking.user_id
                and favorite.offerid = booking.offer_id
        join {{ ref('mrt_global__offer') }} as offer on favorite.offerid = offer.offer_id
        join {{ source('raw', 'applicative_database_stock') }} as stock on favorite.offerid = stock.offer_id
        join {{ ref('mrt_global__user') }} as enruser on favorite.userid = enruser.user_id
        join {{ source('raw','subcategories') }} as subcategories on subcategories.id = offer.offer_subcategory_id

    where
        datecreated <= date_sub(date('{{ ds() }}'), interval 8 day)
        and datecreated > date_sub(date('{{ ds() }}'), interval 15 day)
        and booking.offer_id is NULL
        and booking.user_id is NULL
        and offer.offer_is_bookable = TRUE
        and (stock.stock_beginning_date > date_sub(date('{{ ds() }}'), interval 1 day) or stock.stock_beginning_date is NULL)
        and enruser.user_is_current_beneficiary = TRUE
        and enruser.last_booking_date >= date_sub(date('{{ ds() }}'), interval 8 day)
        and (
            enruser.total_theoretical_remaining_credit
        ) > stock.stock_price
        and (
            (subcategories.is_digital_deposit and (100 - enruser.total_last_deposit_digital_goods_amount_spent) > stock.stock_price)
            or not subcategories.is_digital_deposit
        )
)

select
    date('{{ ds() }}') as execution_date,
    user_id,
    array_agg(
        struct(
            offer_id,
            offer_name,
            subcategory,
            user_bookings_for_this_subcat
        )
        order by
            user_bookings_for_this_subcat asc
        LIMIT
        1
    )[offset(0)].*
from
    favorites
group by
    user_id
