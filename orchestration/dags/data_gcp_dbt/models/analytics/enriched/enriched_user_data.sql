with date_of_first_bookings as (
    select
        booking.user_id,
        MIN(booking.booking_creation_date) as first_booking_date
    from

        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on stock.stock_id = booking.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
    where
        booking.booking_is_cancelled is FALSE
    group by
        user_id
),

date_of_second_bookings_ranked_booking_data as (
    select
        booking.user_id,
        booking.booking_creation_date,
        RANK() over (
            partition by booking.user_id
            order by
                booking.booking_creation_date asc
        ) as rank_booking
    from
        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on stock.stock_id = booking.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
    where booking.booking_is_cancelled is FALSE
),

date_of_second_bookings as (
    select
        user_id,
        booking_creation_date as second_booking_date
    from
        date_of_second_bookings_ranked_booking_data
    where
        rank_booking = 2
),

date_of_bookings_on_third_product_tmp as (
    select
        booking.*,
        offer.offer_subcategoryid,
        offer.offer_name,
        offer.offer_id,
        RANK() over (
            partition by
                booking.user_id,
                offer.offer_subcategoryid
            order by
                booking.booking_creation_date
        ) as rank_booking_in_cat
    from
        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on booking.stock_id = stock.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
    where booking.booking_is_cancelled is FALSE
),

date_of_bookings_on_third_product_ranked_data as (
    select
        *,
        RANK() over (
            partition by user_id
            order by
                booking_creation_date
        ) as rank_cat
    from
        date_of_bookings_on_third_product_tmp
    where
        rank_booking_in_cat = 1
),

date_of_bookings_on_third_product as (
    select
        user_id,
        booking_creation_date as booking_on_third_product_date
    from
        date_of_bookings_on_third_product_ranked_data
    where
        rank_cat = 3
),

number_of_bookings as (
    select
        booking.user_id,
        COUNT(booking.booking_id) as number_of_bookings
    from
        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on stock.stock_id = booking.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
    group by
        user_id
),

number_of_non_cancelled_bookings as (
    select
        booking.user_id,
        COUNT(booking.booking_id) as number_of_bookings
    from
        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on stock.stock_id = booking.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
            and not booking.booking_is_cancelled
    group by
        user_id
),

actual_amount_spent as (
    select
        user.user_id,
        COALESCE(
            SUM(
                booking.booking_intermediary_amount
            ),
            0
        ) as actual_amount_spent
    from
        {{ ref('user_beneficiary') }} as user
        left join {{ ref('booking') }} as booking on user.user_id = booking.user_id
            and booking.booking_is_used is TRUE
    group by
        user.user_id
),

theoretical_amount_spent as (
    select
        user.user_id,
        COALESCE(
            SUM(
                booking.booking_intermediary_amount
            ),
            0
        ) as theoretical_amount_spent
    from
        {{ ref('user_beneficiary') }} as user
        left join {{ ref('booking') }} as booking on user.user_id = booking.user_id
            and booking.booking_is_cancelled is FALSE
    group by
        user.user_id
),

theoretical_amount_spent_in_digital_goods_eligible_booking as (
    select
        booking.user_id,
        booking.booking_amount,
        booking.booking_quantity,
        booking.booking_intermediary_amount
    from
        {{ ref('booking') }} as booking
        left join {{ source('raw','applicative_database_stock') }} as stock on booking.stock_id = stock.stock_id
        left join {{ ref('offer') }} as offer on stock.offer_id = offer.offer_id
        inner join {{ source('clean','subcategories') }} as subcategories on offer.offer_subcategoryid = subcategories.id
    where
        subcategories.is_digital_deposit = TRUE
        and offer.offer_url is not NULL
        and booking.booking_is_cancelled is FALSE
),

theoretical_amount_spent_in_digital_goods as (
    select
        user.user_id,
        COALESCE(
            SUM(
                eligible_booking.booking_intermediary_amount
            ),
            0.
        ) as amount_spent_in_digital_goods
    from
        {{ ref('user_beneficiary') }} as user
        left join theoretical_amount_spent_in_digital_goods_eligible_booking eligible_booking on user.user_id = eligible_booking.user_id
    group by
        user.user_id
),

theoretical_amount_spent_in_physical_goods_eligible_booking as (
    select
        booking.user_id,
        booking.booking_amount,
        booking.booking_quantity,
        booking.booking_intermediary_amount
    from
        {{ ref('booking') }} as booking
        left join {{ source('raw','applicative_database_stock') }} as stock on booking.stock_id = stock.stock_id
        left join {{ ref('offer') }} as offer on stock.offer_id = offer.offer_id
        inner join {{ source('clean','subcategories') }} as subcategories on offer.offer_subcategoryid = subcategories.id
    where
        subcategories.is_physical_deposit = TRUE
        and offer.offer_url is NULL
        and booking.booking_is_cancelled is FALSE
),

theoretical_amount_spent_in_physical_goods as (
    select
        user.user_id,
        COALESCE(
            SUM(
                eligible_booking.booking_intermediary_amount
            ),
            0.
        ) as amount_spent_in_physical_goods
    from
        {{ ref('user_beneficiary') }} as user
        left join theoretical_amount_spent_in_physical_goods_eligible_booking eligible_booking on user.user_id = eligible_booking.user_id
    group by
        user.user_id
),

theoretical_amount_spent_in_outings_eligible_booking as (
    select
        booking.user_id,
        booking.booking_amount,
        booking.booking_quantity,
        booking.booking_intermediary_amount
    from
        {{ ref('booking') }} as booking
        left join {{ source('raw','applicative_database_stock') }} as stock on booking.stock_id = stock.stock_id
        left join {{ ref('offer') }} as offer on stock.offer_id = offer.offer_id
        inner join {{ source('clean','subcategories') }} as subcategories on offer.offer_subcategoryid = subcategories.id
    where
        subcategories.is_event = TRUE
        and booking.booking_is_cancelled is FALSE
),

theoretical_amount_spent_in_outings as (
    select
        user.user_id,
        COALESCE(
            SUM(
                eligible_booking.booking_intermediary_amount
            ),
            0.
        ) as amount_spent_in_outings
    from
        {{ ref('user_beneficiary') }} as user
        left join theoretical_amount_spent_in_outings_eligible_booking eligible_booking on user.user_id = eligible_booking.user_id
    group by
        user.user_id
),

last_booking_date as (
    select
        booking.user_id,
        MAX(booking.booking_creation_date) as last_booking_date
    from
        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on stock.stock_id = booking.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
    group by
        user_id
),

first_paid_booking_date as (
    select
        booking.user_id,
        MIN(booking.booking_creation_date) as booking_creation_date_first
    from
        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on stock.stock_id = booking.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
            and COALESCE(booking.booking_amount, 0) > 0
    group by
        user_id
),

first_booking_type_bookings_ranked as (
    select
        booking.booking_id,
        booking.user_id,
        offer.offer_subcategoryid,
        RANK() over (
            partition by booking.user_id
            order by
                booking.booking_creation_date,
                booking.booking_id asc
        ) as rank_booking
    from
        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on booking.stock_id = stock.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
),

first_booking_type as (
    select
        user_id,
        offer_subcategoryid as first_booking_type
    from
        first_booking_type_bookings_ranked
    where
        rank_booking = 1
),

first_paid_booking_type_paid_bookings_ranked as (
    select
        booking.booking_id,
        booking.user_id,
        offer.offer_subcategoryid,
        RANK() over (
            partition by booking.user_id
            order by
                booking.booking_creation_date
        ) as rank_booking
    from
        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on booking.stock_id = stock.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
            and booking.booking_amount > 0
),

first_paid_booking_type as (
    select
        user_id,
        offer_subcategoryid as first_paid_booking_type
    from
        first_paid_booking_type_paid_bookings_ranked
    where
        rank_booking = 1
),

count_distinct_types as (
    select
        booking.user_id,
        COUNT(distinct offer.offer_subcategoryid) as cnt_distinct_types
    from
        {{ ref('booking') }} as booking
        join {{ source('raw','applicative_database_stock') }} as stock on booking.stock_id = stock.stock_id
        join {{ ref('offer') }} as offer on offer.offer_id = stock.offer_id
            and not booking_is_cancelled
    group by
        user_id
),

user_agg_deposit_data_user_deposit_agg as (
    select
        userid,
        MIN(datecreated) as user_first_deposit_creation_date,
        MIN(amount) as user_first_deposit_amount,
        MAX(amount) as user_last_deposit_amount,
        MAX(expirationdate) as user_last_deposit_expiration_date,
        SUM(amount) as user_total_deposit_amount
    from
        {{ ref('deposit') }}
    group by
        1
),

user_agg_deposit_data as (
    select
        user_deposit_agg.*,
        case
            when user_last_deposit_amount < 300 then 'GRANT_15_17'
            else 'GRANT_18'
        end as user_current_deposit_type,
        case
            when user_first_deposit_amount < 300 then 'GRANT_15_17'
            else 'GRANT_18'
        end as user_first_deposit_type
    from
        user_agg_deposit_data_user_deposit_agg user_deposit_agg
),

last_deposit as (
    select
        deposit.userid as user_id,
        deposit.id as deposit_id
    from
        {{ ref('deposit') }} as deposit
    qualify ROW_NUMBER() over (partition by deposit.userid order by deposit.datecreated desc, id desc) = 1
),

amount_spent_last_deposit as (
    select
        booking.deposit_id,
        last_deposit.user_id,
        COALESCE(
            SUM(booking_intermediary_amount),
            0
        ) as deposit_theoretical_amount_spent,
        SUM(case
            when booking_is_used is TRUE
                then booking_intermediary_amount
            else 0
        end) as deposit_actual_amount_spent,
        SUM(case
            when subcategories.is_digital_deposit = TRUE and offer.offer_url is not NULL
                then booking_intermediary_amount
            else 0
        end) as deposit_theoretical_amount_spent_in_digital_goods
    from
        {{ ref('booking') }} as booking
        join last_deposit
            on last_deposit.deposit_id = booking.deposit_id
        left join {{ source('raw','applicative_database_stock') }} as stock
            on booking.stock_id = stock.stock_id
        left join {{ ref('offer') }} as offer
            on stock.offer_id = offer.offer_id
        inner join {{ source('clean','subcategories') }} as subcategories
            on offer.offer_subcategoryid = subcategories.id
    where booking_is_cancelled is FALSE
    group by
        deposit_id,
        last_deposit.user_id
),

themes_subscribed as (
    select
        user_id,
        currently_subscribed_themes,
        case when (currently_subscribed_themes is NULL or currently_subscribed_themes = '') then FALSE else TRUE end as is_theme_subscribed
    from {{ source('analytics','app_native_logs') }}
    where technical_message_id = "subscription_update"
    qualify ROW_NUMBER() over (partition by user_id order by partition_date desc) = 1
)

select
    user.user_id,
    user.user_department_code,
    user.user_postal_code,
    user.user_activity,
    user.user_civility,
    user.user_school_type,
    user.user_activation_date,
    user_agg_deposit_data.user_first_deposit_creation_date as user_deposit_creation_date,
    user_agg_deposit_data.user_first_deposit_type as user_first_deposit_type,
    user_agg_deposit_data.user_total_deposit_amount,
    user_agg_deposit_data.user_current_deposit_type,
    user.user_cultural_survey_filled_date as first_connection_date,
    date_of_first_bookings.first_booking_date,
    date_of_second_bookings.second_booking_date,
    date_of_bookings_on_third_product.booking_on_third_product_date,
    COALESCE(number_of_bookings.number_of_bookings, 0) as booking_cnt,
    COALESCE(
        number_of_non_cancelled_bookings.number_of_bookings,
        0
    ) as no_cancelled_booking,
    DATE_DIFF(CURRENT_DATE(), CAST(user.user_activation_date as DATE), day) as user_seniority,
    actual_amount_spent.actual_amount_spent,
    theoretical_amount_spent.theoretical_amount_spent,
    theoretical_amount_spent_in_digital_goods.amount_spent_in_digital_goods,
    theoretical_amount_spent_in_physical_goods.amount_spent_in_physical_goods,
    theoretical_amount_spent_in_outings.amount_spent_in_outings,
    amount_spent_last_deposit.deposit_theoretical_amount_spent as last_deposit_theoretical_amount_spent,
    amount_spent_last_deposit.deposit_theoretical_amount_spent_in_digital_goods as last_deposit_theoretical_amount_spent_in_digital_goods,
    amount_spent_last_deposit.deposit_actual_amount_spent as last_deposit_actual_amount_spent,
    user_last_deposit_amount,
    user_last_deposit_amount - amount_spent_last_deposit.deposit_theoretical_amount_spent as user_theoretical_remaining_credit,
    user.user_humanized_id,
    last_booking_date.last_booking_date,
    first_paid_booking_date.booking_creation_date_first,
    DATE_DIFF(
        date_of_first_bookings.first_booking_date,
        user_agg_deposit_data.user_first_deposit_creation_date,
        day
    ) as days_between_activation_date_and_first_booking_date,
    DATE_DIFF(
        first_paid_booking_date.booking_creation_date_first,
        user_agg_deposit_data.user_first_deposit_creation_date,
        day
    ) as days_between_activation_date_and_first_booking_paid,
    first_booking_type.first_booking_type,
    first_paid_booking_type.first_paid_booking_type,
    count_distinct_types.cnt_distinct_types as cnt_distinct_type_booking,
    user.user_is_active,
    user_suspension.action_history_reason as user_suspension_reason,
    user_agg_deposit_data.user_first_deposit_amount as user_deposit_initial_amount,
    user_agg_deposit_data.user_last_deposit_expiration_date as user_deposit_expiration_date,
    case
        when (
            TIMESTAMP(
                user_agg_deposit_data.user_last_deposit_expiration_date
            ) >= CURRENT_TIMESTAMP()
            and COALESCE(amount_spent_last_deposit.deposit_actual_amount_spent, 0) < user_agg_deposit_data.user_last_deposit_amount
        )
        and user_is_active then TRUE
        else FALSE
    end as user_is_current_beneficiary,
    user.user_age,
    user.user_birth_date,
    user.user_has_enabled_marketing_email,
    user.user_iris_internal_id,
    themes_subscribed.currently_subscribed_themes,
    themes_subscribed.is_theme_subscribed,
    user.user_region_name,
    user.user_city,
    user.user_epci,
    user.user_academy_name,
    user.user_density_label,
    user.user_macro_density_label,
    user.user_is_in_qpv,
    user.user_is_unemployed,
    user.user_is_priority_public
from
    {{ ref('user_beneficiary') }} as user
    left join date_of_first_bookings on user.user_id = date_of_first_bookings.user_id
    left join date_of_second_bookings on user.user_id = date_of_second_bookings.user_id
    left join date_of_bookings_on_third_product on user.user_id = date_of_bookings_on_third_product.user_id
    left join number_of_bookings on user.user_id = number_of_bookings.user_id
    left join number_of_non_cancelled_bookings on user.user_id = number_of_non_cancelled_bookings.user_id
    left join actual_amount_spent on user.user_id = actual_amount_spent.user_id
    left join theoretical_amount_spent on user.user_id = theoretical_amount_spent.user_id
    left join theoretical_amount_spent_in_digital_goods on user.user_id = theoretical_amount_spent_in_digital_goods.user_id
    left join theoretical_amount_spent_in_physical_goods on user.user_id = theoretical_amount_spent_in_physical_goods.user_id
    left join theoretical_amount_spent_in_outings on user.user_id = theoretical_amount_spent_in_outings.user_id
    left join last_booking_date on last_booking_date.user_id = user.user_id
    left join first_paid_booking_date on user.user_id = first_paid_booking_date.user_id
    left join first_booking_type on user.user_id = first_booking_type.user_id
    left join first_paid_booking_type on user.user_id = first_paid_booking_type.user_id
    left join count_distinct_types on user.user_id = count_distinct_types.user_id
    left join
        {{ ref('user_suspension') }}
            as user_suspension
        on user_suspension.user_id = user.user_id
            and action_history_rk = 1
    left join amount_spent_last_deposit on amount_spent_last_deposit.user_id = user.user_id
    left join themes_subscribed on themes_subscribed.user_id = user.user_id
    join user_agg_deposit_data on user.user_id = user_agg_deposit_data.userid
where
    (
        user.user_is_active
        or user_suspension.action_history_reason = 'upon user request'
    )
