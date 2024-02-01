WITH weeks AS (
    SELECT
        *
    FROM
        UNNEST(
            GENERATE_DATE_ARRAY('2018-01-01', CURRENT_DATE, INTERVAL 1 WEEK)
        ) AS week
),

user_active_weeks AS (
    SELECT
        cultural_partner.partner_id
        ,cultural_partner.offerer_id
        ,date(partner_creation_date) as partner_creation_date
        ,weeks.week AS active_week
        ,DATE_DIFF(CURRENT_DATE, partner_creation_date, WEEK(MONDAY)) AS nb_weeks_since_today
    FROM `{{ bigquery_analytics_dataset }}.enriched_cultural_partner_data` cultural_partner
    JOIN weeks ON weeks.week >= DATE_TRUNC(date(partner_creation_date), WEEK(MONDAY))
),

--only keep max number bookable offers per week
bookable_per_week AS(
    SELECT 
        partner_id 
        ,DATE_TRUNC(partition_date, WEEK(MONDAY)) as date_week 
        ,MAX(individual_bookable_offers) as individual_bookable_offers
        ,MAX(collective_bookable_offers) as collective_bookable_offers
        ,MAX(total_bookable_offers) as bookable_offers
    FROM `{{ bigquery_analytics_dataset }}.bookable_partner_history` 
    GROUP BY 1,2
),

weekly_bookings_and_bookable_offers AS (
    SELECT
        user_active_weeks.*
        ,DATE_DIFF(user_active_weeks.active_week,DATE_TRUNC(partner_creation_date,WEEK(MONDAY)),WEEK) AS nb_weeks_since_partner_created
        ,COALESCE(individual_bookable_offers,0) AS nb_individual_bookable_offers
        ,COALESCE(collective_bookable_offers,0) AS nb_collective_bookable_offers
        ,COALESCE(bookable_offers,0) AS nb_bookable_offers
        ,COALESCE(COUNT(distinct booking_id), 0) AS nb_individual_booking
        ,COALESCE(COUNT(distinct collective_booking_id), 0) AS nb_collective_booking
        ,COALESCE(COUNT(distinct booking_id), 0) + COALESCE(COUNT(distinct collective_booking_id), 0) AS nb_booking
    FROM user_active_weeks
        LEFT JOIN bookable_per_week ON bookable_per_week.partner_id=user_active_weeks.partner_id 
        AND user_active_weeks.active_week = date_week
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_booking_data` booking ON booking.partner_id=user_active_weeks.partner_id
        AND user_active_weeks.active_week = DATE_TRUNC(DATE(booking_creation_date),WEEK(MONDAY))
        AND booking_is_cancelled IS FALSE
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_collective_booking_data` collective_booking ON collective_booking.offerer_id=user_active_weeks.offerer_id --attention changer en partner
        AND user_active_weeks.active_week = DATE_TRUNC(DATE(collective_booking_creation_date),WEEK(MONDAY))
        AND collective_booking_is_cancelled = "FALSE"
    GROUP BY 1,2,3,4,5,6,7,8,9
),

add_weekly_offers AS (
    SELECT
        weekly_bookings_and_bookable_offers.*
        ,COALESCE(COUNT(distinct offer.offer_id), 0) AS nb_individual_offer
        ,COALESCE(COUNT(distinct collective_offer.collective_offer_id), 0) AS nb_collective_offer
        ,COALESCE(COUNT(distinct offer.offer_id), 0) + COALESCE(COUNT(distinct collective_offer.collective_offer_id), 0) AS nb_offer
    FROM weekly_bookings_and_bookable_offers
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` offer ON offer.partner_id=weekly_bookings_and_bookable_offers.partner_id
        AND weekly_bookings_and_bookable_offers.active_week = DATE_TRUNC(DATE(offer_creation_date),WEEK(MONDAY))
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_collective_offer_data` collective_offer ON collective_offer.partner_id=weekly_bookings_and_bookable_offers.partner_id
        AND weekly_bookings_and_bookable_offers.active_week = DATE_TRUNC(DATE(collective_offer_creation_date),WEEK(MONDAY))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

add_cumul_bookings_and_offers AS (
    SELECT 
        *
        ,SUM(nb_individual_booking) OVER(PARTITION BY partner_id ORDER BY active_week ASC) as cum_nb_individual_booking
        ,SUM(nb_collective_booking) OVER(PARTITION BY partner_id ORDER BY active_week ASC) as cum_nb_collective_booking
        ,SUM(nb_booking) OVER(PARTITION BY partner_id ORDER BY active_week ASC) as cum_nb_booking
        ,SUM(nb_individual_offer) OVER(PARTITION BY partner_id ORDER BY active_week ASC) as cum_nb_individual_offer
        ,SUM(nb_collective_offer) OVER(PARTITION BY partner_id ORDER BY active_week ASC) as cum_nb_collective_offer
        ,SUM(nb_offer) OVER(PARTITION BY partner_id ORDER BY active_week ASC) as cum_nb_offer
    FROM add_weekly_offers
)

SELECT 
        partner_id
        ,offerer_id
        ,partner_creation_date
        ,active_week
        ,nb_weeks_since_today
        ,nb_weeks_since_partner_created
-- bookable offers this week
        ,nb_individual_bookable_offers
        ,nb_collective_bookable_offers
        ,nb_bookable_offers
-- offers created this week
        ,nb_individual_offer
        ,nb_collective_offer
        ,nb_offer
-- bookings this week
        ,nb_individual_booking
        ,nb_collective_booking
        ,nb_booking
-- offers created cumul
       ,cum_nb_individual_offer
       ,cum_nb_collective_offer
       ,cum_nb_offer
-- bookings cumul
        ,cum_nb_individual_booking
        ,cum_nb_collective_booking
        ,cum_nb_booking
FROM add_cumul_bookings_and_offers