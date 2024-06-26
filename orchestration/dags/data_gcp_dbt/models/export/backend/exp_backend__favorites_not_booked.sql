{{
    config(
        materialized = "incremental" if target.profile != "CI" else "view",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "execution_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
}}

WITH favorites as (
    SELECT
        DISTINCT favorite.userId as user_id,
        offerId as offer_id,
        offer.offer_name,
        offer.offer_subcategory_id as subcategory,
        (
            SELECT
                count(*)
            FROM
                {{ ref('mrt_global__booking') }}
            WHERE
                offer_subcategory_id = offer.offer_subcategory_id
                AND user_id = favorite.userId
        ) as user_bookings_for_this_subcat,
    FROM
        {{ source('raw', 'applicative_database_favorite') }} as favorite
        LEFT JOIN {{ ref('mrt_global__booking') }} as booking ON favorite.userId = booking.user_id
        AND favorite.offerId = booking.offer_id
        JOIN {{ ref('mrt_global__offer') }} as offer ON favorite.offerId = offer.offer_id
        JOIN {{ source('raw', 'applicative_database_stock') }} as stock ON favorite.offerId = stock.offer_id
        JOIN {{ ref('enriched_user_data') }} as enruser ON favorite.userId = enruser.user_id
        JOIN {{ source('clean','subcategories') }} AS subcategories ON subcategories.id = offer.offer_subcategory_id

    WHERE
        dateCreated <= DATE_SUB(DATE('{{ ds() }}'), INTERVAL 8 DAY)
        AND dateCreated > DATE_SUB(DATE('{{ ds() }}'), INTERVAL 15 DAY)
        AND booking.offer_id IS NULL
        AND booking.user_id IS NULL
        AND offer.offer_is_bookable = True
        AND ( stock.stock_beginning_date > DATE_SUB(DATE('{{ ds() }}'), INTERVAL 1 DAY) OR stock.stock_beginning_date is NULL)
        AND enruser.user_is_current_beneficiary = True
        AND enruser.last_booking_date >= DATE_SUB(DATE('{{ ds() }}'), INTERVAL 8 DAY)
        AND (
            enruser.user_theoretical_remaining_credit
        ) > stock.stock_price
        AND (
                (subcategories.is_digital_deposit AND (100 - enruser.last_deposit_theoretical_amount_spent_in_digital_goods) > stock.stock_price)
                OR NOT subcategories.is_digital_deposit
            )
        -- Hotfix before the dbt weekly DAG is fixed: Add condition to ensure the model runs only on Mondays
        AND EXTRACT(DAYOFWEEK FROM DATE('{{ ds() }}')) = 2
)
SELECT
    DATE('{{ ds() }}') as execution_date,
    user_id,
    ARRAY_AGG(
        STRUCT(
            offer_id,
            offer_name,
            subcategory,
            user_bookings_for_this_subcat
        )
        ORDER BY
            user_bookings_for_this_subcat ASC
        LIMIT
            1
    ) [OFFSET(0)].*
FROM
    favorites
GROUP BY
    user_id