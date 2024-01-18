WITH fraud_users AS (SELECT
    user_id
FROM `{{ bigquery_clean_dataset }}`.user_suspension
QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY action_date DESC) = 1 -- ceux qui n'ont pas été unsuspended ensuite
AND action_type = 'USER_SUSPENDED'
AND action_history_json_data LIKE '%fraud%')


,individual_offers_created AS (SELECT
    enriched_cultural_partner_data.partner_id
    ,enriched_cultural_partner_data.partner_type
    ,enriched_cultural_partner_data.cultural_sector
    ,COUNT(offer_id) AS individual_offers_created_cnt
    ,COUNT(CASE WHEN DATE_DIFF(CURRENT_DATE,offer_creation_date,MONTH) <= 2 THEN offer_id END) AS individual_offers_created_last_2_month
    ,COUNT(CASE WHEN DATE_DIFF(CURRENT_DATE,offer_creation_date,MONTH) <= 6 THEN offer_id END) AS individual_offers_created_last_6_month
    ,COUNT(CASE WHEN DATE_DIFF(last_bookable_offer_date,offer_creation_date,MONTH) <= 2 THEN offer_id END) AS individual_offers_created_2_month_before_last_bookable
    ,COUNT(CASE WHEN DATE_DIFF(last_bookable_offer_date,offer_creation_date,MONTH) <= 6 THEN offer_id END) AS individual_offers_created_6_month_before_last_bookable
FROM `{{ bigquery_analytics_dataset }}`.enriched_cultural_partner_data
JOIN `{{ bigquery_analytics_dataset }}`.partner_type_bookability_frequency USING(partner_type)
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data ON enriched_cultural_partner_data.partner_id = enriched_offer_data.partner_id
GROUP BY 1,2,3)

,individual_bookings AS (
SELECT
    enriched_cultural_partner_data.partner_id
    ,enriched_cultural_partner_data.partner_type
    ,enriched_cultural_partner_data.cultural_sector
    ,COALESCE(COUNT(DISTINCT user_id),0) AS unique_users
    ,COALESCE(COUNT(DISTINCT CASE WHEN enriched_booking_data.user_id IN (SELECT DISTINCT user_id FROM fraud_users) THEN enriched_booking_data.user_id ELSE NULL END),0) AS unique_fraud_users
    ,COALESCE(COUNT(booking_id),0) AS individual_bookings_cnt
    ,COALESCE(SUM(CASE WHEN booking_is_used THEN booking_intermediary_amount ELSE NULL END),0) AS real_individual_revenue
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(CURRENT_DATE,booking_creation_date,MONTH) <= 2 THEN booking_id END),0) AS individual_bookings_last_2_month
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(CURRENT_DATE,booking_creation_date,MONTH) <= 6 THEN booking_id END),0) AS individual_bookings_last_6_month
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(last_bookable_offer_date,booking_creation_date,MONTH) <= 2 THEN booking_id END),0) AS individual_bookings_2_month_before_last_bookable
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(last_bookable_offer_date,booking_creation_date,MONTH) <= 6 THEN booking_id END),0) AS individual_bookings_6_month_before_last_bookable
FROM `{{ bigquery_analytics_dataset }}`.enriched_cultural_partner_data
JOIN `{{ bigquery_analytics_dataset }}`.partner_type_bookability_frequency USING(partner_type)
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_booking_data ON enriched_cultural_partner_data.partner_id = enriched_booking_data.partner_id
    AND NOT booking_is_cancelled
GROUP BY 1,2,3)

,collective_offers_created AS (SELECT
    enriched_cultural_partner_data.partner_id
    ,enriched_cultural_partner_data.partner_type
    ,enriched_cultural_partner_data.cultural_sector
    ,COALESCE(COUNT(collective_offer_id),0) AS collective_offers_created_cnt
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(CURRENT_DATE,collective_offer_creation_date,MONTH) <= 2 THEN collective_offer_id END),0) AS collective_offers_created_last_2_month
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(CURRENT_DATE,collective_offer_creation_date,MONTH) <= 6 THEN collective_offer_id END),0) AS collective_offers_created_last_6_month
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(last_bookable_offer_date,collective_offer_creation_date,MONTH) <= 2 THEN collective_offer_id END),0) AS collective_offers_created_2_month_before_last_bookable
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(last_bookable_offer_date,collective_offer_creation_date,MONTH) <= 6 THEN collective_offer_id END),0) AS collective_offers_created_6_month_before_last_bookable
FROM `{{ bigquery_analytics_dataset }}`.enriched_cultural_partner_data
JOIN `{{ bigquery_analytics_dataset }}`.partner_type_bookability_frequency USING(partner_type)
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_collective_offer_data ON enriched_cultural_partner_data.partner_id = enriched_collective_offer_data.partner_id
GROUP BY 1,2,3)

, collective_bookings1 AS (
SELECT
    CASE WHEN enriched_venue_data.venue_is_permanent THEN CONCAT("venue-",enriched_collective_booking_data.venue_id)
         ELSE CONCAT("offerer-", enriched_collective_booking_data.offerer_id) END AS partner_id
         ,enriched_collective_booking_data.*
FROM `{{ bigquery_analytics_dataset }}`.enriched_collective_booking_data
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data USING(venue_id)
WHERE NOT collective_booking_status = 'CANCELLED')

,collective_bookings AS (
SELECT
    enriched_cultural_partner_data.partner_id
    ,enriched_cultural_partner_data.partner_type
    ,enriched_cultural_partner_data.cultural_sector
    ,COALESCE(COUNT(collective_booking_id),0) AS collective_bookings_cnt
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(CURRENT_DATE,collective_booking_creation_date,MONTH) <= 2 THEN collective_booking_id END),0) AS collective_bookings_last_2_month
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(CURRENT_DATE,collective_booking_creation_date,MONTH) <= 6 THEN collective_booking_id END),0) AS collective_bookings_last_6_month
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(last_bookable_offer_date,collective_booking_creation_date,MONTH) <= 2 THEN collective_booking_id END),0) AS collective_bookings_2_month_before_last_bookable
    ,COALESCE(COUNT(CASE WHEN DATE_DIFF(CURRENT_DATE,collective_booking_creation_date,MONTH) <= 6 THEN collective_booking_id END),0) AS collective_bookings_6_month_before_last_bookable
    ,COALESCE(SUM(CASE WHEN collective_booking_status IN ('USED','REIMBURSED') THEN booking_amount ELSE NULL END),0) AS real_collective_revenue
FROM `{{ bigquery_analytics_dataset }}`.enriched_cultural_partner_data
JOIN `{{ bigquery_analytics_dataset }}`.partner_type_bookability_frequency USING(partner_type)
LEFT JOIN collective_bookings1 ON enriched_cultural_partner_data.partner_id = collective_bookings1.partner_id
GROUP BY 1,2,3)

,favorites1 AS (SELECT DISTINCT
    CASE WHEN enriched_venue_data.venue_is_permanent THEN CONCAT("venue-",enriched_venue_data.venue_id)
         ELSE CONCAT("offerer-", venue_managing_offerer_id) END AS partner_id
    ,applicative_database_favorite.*
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data ON enriched_venue_data.venue_id = enriched_offer_data.venue_id
LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_favorite ON enriched_offer_data.offer_id = applicative_database_favorite.offerId)

,favorites AS (SELECT
    enriched_cultural_partner_data.partner_id
    ,enriched_cultural_partner_data.partner_type
    ,enriched_cultural_partner_data.cultural_sector
    ,COALESCE(COUNT(*),0) AS favorites_cnt
FROM favorites1
JOIN `{{ bigquery_analytics_dataset }}`.enriched_cultural_partner_data USING(partner_id)
JOIN `{{ bigquery_analytics_dataset }}`.partner_type_bookability_frequency ON enriched_cultural_partner_data.partner_type = partner_type_bookability_frequency.partner_type
GROUP BY 1,2,3)

,consultations AS (
SELECT
    CASE WHEN venue.venue_is_permanent THEN CONCAT("venue-",venue.venue_id)
         ELSE CONCAT("offerer-", venue_managing_offerer_id) END AS partner_id
    , sum(cnt_events) as total_consultation
    , COALESCE(SUM(CASE WHEN DATE_DIFF(CURRENT_DATE,event_date,MONTH) <= 2 THEN cnt_events END)) as consult_last_2_month
    , COALESCE(SUM(CASE WHEN DATE_DIFF(CURRENT_DATE,event_date,MONTH) <= 6 THEN cnt_events END)) as consult_last_6_month
    , COALESCE(SUM(CASE WHEN DATE_DIFF(last_bookable_offer_date,event_date,MONTH) <= 2 THEN cnt_events END)) as consult_2_month_before_last_bookable
    , COALESCE(SUM(CASE WHEN DATE_DIFF(last_bookable_offer_date,event_date,MONTH) <= 6 THEN cnt_events END)) as consult_6_month_before_last_bookable
FROM `{{ bigquery_analytics_dataset }}`.aggregated_daily_offer_consultation_data consult
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data venue on consult.venue_id = venue.venue_id
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_cultural_partner_data on (CASE WHEN venue.venue_is_permanent THEN CONCAT("venue-",venue.venue_id) ELSE CONCAT("offerer-", venue_managing_offerer_id) END) = enriched_cultural_partner_data.partner_id
GROUP BY 1
)

,adage_status AS (
SELECT
    DISTINCT CASE WHEN enriched_venue_data.venue_is_permanent THEN CONCAT("venue-",enriched_venue_data.venue_id)
     ELSE CONCAT("offerer-", venue_managing_offerer_id) END AS partner_id
    ,first_dms_adage_status
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_offerer_data on enriched_venue_data.venue_managing_offerer_id = enriched_offerer_data.offerer_id
)

,siren_status AS (SELECT DISTINCT
    enriched_venue_data.partner_id
    ,CASE WHEN Etatadministratifunitelegale = 'A' THEN TRUE ELSE FALSE END AS has_active_siren
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
JOIN `{{ bigquery_analytics_dataset }}`.enriched_offerer_data ON enriched_venue_data.venue_managing_offerer_id = enriched_offerer_data.offerer_id
LEFT JOIN `{{ bigquery_analytics_dataset }}`.siren_data ON enriched_offerer_data.offerer_siren = siren_data.siren )

,rejected_offers AS (SELECT
    CASE WHEN enriched_venue_data.venue_is_permanent THEN CONCAT("venue-",enriched_venue_data.venue_id)
         ELSE CONCAT("offerer-", venue_managing_offerer_id) END AS partner_id
    ,COALESCE(COUNT(*),0) AS offers_cnt
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer ON applicative_database_offer.venue_id = enriched_venue_data.venue_id
WHERE offer_validation = 'REJECTED'
GROUP BY 1)

,providers AS (SELECT
    CASE WHEN enriched_venue_data.venue_is_permanent THEN CONCAT("venue-",enriched_venue_data.venue_id)
         ELSE CONCAT("offerer-", venue_managing_offerer_id) END AS partner_id
    ,CASE WHEN provider_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_provider
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_provider_data ON enriched_venue_provider_data.venue_id = enriched_venue_data.venue_id AND is_active )

--- On estime que si une structure a un lieu rattaché à un point de remboursement, tous les lieux de la structure le sont
,reimbursment_point1 AS (SELECT DISTINCT
    venue_managing_offerer_id AS offerer_id
    ,enriched_venue_data.venue_id
    ,venue_is_permanent
    ,reimbursement_point_link_beginning_date
    ,reimbursement_point_link_ending_date
    ,RANK() OVER(PARTITION BY venue_managing_offerer_id,enriched_venue_data.venue_id ORDER BY reimbursement_point_link_beginning_date DESC) AS rang
FROM `{{ bigquery_analytics_dataset }}`.enriched_venue_data
LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue_reimbursement_point_link ON enriched_venue_data.venue_id = applicative_database_venue_reimbursement_point_link.venue_id)

,reimbursment_point2 AS (SELECT
    offerer_id
    ,venue_id
    ,venue_is_permanent
    ,COALESCE(COUNT(CASE WHEN reimbursement_point_link_beginning_date IS NOT NULL THEN 1 ELSE 0 END)) AS nb_reimbursment_point
FROM reimbursment_point1
WHERE rang = 1
AND reimbursement_point_link_ending_date IS NULL
GROUP BY 1,2,3)


,reimbursment_point AS (SELECT
    CASE WHEN venue_is_permanent THEN CONCAT("venue-",venue_id)
         ELSE CONCAT("offerer-", offerer_id) END AS partner_id
    ,SUM(nb_reimbursment_point) AS nb_reimbursment_point
FROM reimbursment_point2
GROUP BY 1)

, bookable as (
SELECT
    CASE WHEN enriched_venue_data.venue_is_permanent THEN CONCAT("venue-",bookable_venue_history.venue_id)
         ELSE CONCAT("offerer-", bookable_venue_history.offerer_id) END AS partner_id,
    max(partition_date) last_bookable_date,
FROM `{{ bigquery_analytics_dataset }}`.bookable_venue_history
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data on bookable_venue_history.venue_id = enriched_venue_data.venue_id
WHERE total_bookable_offers <> 0
GROUP BY 1
)

, churned as (
SELECT
    bookable.partner_id,
    last_bookable_date,
    enriched_cultural_partner_data.cultural_sector,
    median_bookability_frequency,
    DATE_DIFF(current_date(), last_bookable_date, DAY) days_since_last_bookable_offer
FROM bookable
JOIN `{{ bigquery_analytics_dataset }}`.enriched_cultural_partner_data on bookable.partner_id = enriched_cultural_partner_data.partner_id
JOIN `{{ bigquery_analytics_dataset }}`.cultural_sector_bookability_frequency on enriched_cultural_partner_data.cultural_sector = cultural_sector_bookability_frequency.cultural_sector
)

, churn_segmentation as (
SELECT
    partner_id,
    last_bookable_date,
    cultural_sector,
    days_since_last_bookable_offer,
    CASE WHEN median_bookability_frequency = 13
        THEN
        CASE WHEN days_since_last_bookable_offer < 30 THEN "active"
            WHEN days_since_last_bookable_offer < 60 THEN "at_risk"
            ELSE "churned" END
        WHEN median_bookability_frequency > 6
        THEN
        CASE WHEN days_since_last_bookable_offer < 60 THEN "active"
            WHEN days_since_last_bookable_offer < 120 THEN "at_risk"
            ELSE "churned" END
        WHEN median_bookability_frequency <= 6
        THEN
        CASE WHEN days_since_last_bookable_offer < 90 THEN "active"
            WHEN days_since_last_bookable_offer < 180 THEN "at_risk"
            ELSE "churned" END
        ELSE "not-activated"
        END AS partner_segmentation
FROM churned)

SELECT DISTINCT
    enriched_cultural_partner_data.partner_id
    ,enriched_cultural_partner_data.partner_creation_date
    ,DATE_DIFF(CURRENT_DATE,partner_creation_date,MONTH) AS seniority_month
    ,enriched_cultural_partner_data.cultural_sector
    ,enriched_cultural_partner_data.partner_type
    ,CASE WHEN enriched_cultural_partner_data.individual_offers_created > 0 THEN TRUE ELSE FALSE END AS activated_individual_part
    ,CASE WHEN enriched_cultural_partner_data.collective_offers_created > 0 THEN TRUE ELSE FALSE END AS activated_collective_part
    ,COALESCE(individual_offers_created_cnt,0) AS individual_offers_created_cnt
    ,COALESCE(individual_offers_created_last_2_month,0) AS individual_offers_created_last_2_month
    ,COALESCE(individual_offers_created_last_6_month,0) AS individual_offers_created_last_6_month
    ,COALESCE(individual_offers_created_2_month_before_last_bookable,0) AS individual_offers_created_2_month_before_last_bookable
    ,COALESCE(individual_offers_created_6_month_before_last_bookable,0) AS individual_offers_created_6_month_before_last_bookable
    ,COALESCE(collective_offers_created_cnt,0) AS collective_offers_created_cnt
    ,COALESCE(collective_offers_created_last_2_month,0) AS collective_offers_created_last_2_month
    ,COALESCE(collective_offers_created_last_6_month,0) AS collective_offers_created_last_6_month
    ,COALESCE(collective_offers_created_2_month_before_last_bookable,0) AS collective_offers_created_2_month_before_last_bookable
    ,COALESCE(collective_offers_created_6_month_before_last_bookable,0) AS collective_offers_created_6_month_before_last_bookable
    ,COALESCE(individual_bookings_cnt,0) AS individual_bookings_cnt
    ,COALESCE(individual_bookings_last_2_month,0) AS individual_bookings_last_2_month
    ,COALESCE(individual_bookings_last_6_month,0) AS individual_bookings_last_6_month
    ,COALESCE(individual_bookings_2_month_before_last_bookable,0) AS individual_bookings_2_month_before_last_bookable
    ,COALESCE(individual_bookings_6_month_before_last_bookable,0) AS individual_bookings_6_month_before_last_bookable
    ,COALESCE(collective_bookings_cnt,0) AS collective_bookings_cnt
    ,COALESCE(collective_bookings_last_2_month,0) AS collective_bookings_last_2_month
    ,COALESCE(collective_bookings_last_6_month,0) AS collective_bookings_last_6_month
    ,COALESCE(collective_bookings_2_month_before_last_bookable,0) AS collective_bookings_2_month_before_last_bookable
    ,COALESCE(collective_bookings_6_month_before_last_bookable,0) AS collective_bookings_6_month_before_last_bookable
    ,COALESCE(individual_bookings.real_individual_revenue,0) AS real_individual_revenue
    ,COALESCE(collective_bookings.real_collective_revenue,0) AS real_collective_revenue
    ,COALESCE(favorites.favorites_cnt,0) AS favorites_cnt
    ,COALESCE(consultations.total_consultation,0) AS total_consultation
    ,COALESCE(consultations.consult_last_2_month,0) AS consultation_last_2_month
    ,COALESCE(consultations.consult_last_6_month,0) AS consultation_last_6_month
    ,COALESCE(consultations.consult_2_month_before_last_bookable,0) AS consultation_2_month_before_last_bookable
    ,COALESCE(consultations.consult_6_month_before_last_bookable,0) AS consultation_6_month_before_last_bookable
    ,has_active_siren
    ,COALESCE(first_dms_adage_status, "dms_adage_non_depose") AS first_dms_adage_status
    ,COALESCE(rejected_offers.offers_cnt,0) AS rejected_offers_cnt
    ,COALESCE(ROUND(SAFE_DIVIDE(rejected_offers.offers_cnt, individual_offers_created_cnt + rejected_offers.offers_cnt)*100),0) AS rejected_offers_pct
    ,has_provider
    ,CASE WHEN nb_reimbursment_point >= 1 THEN TRUE ELSE FALSE END AS has_reimbursement_point
    ,COALESCE(unique_fraud_users,0) AS unique_fraud_users
    ,ROUND(SAFE_DIVIDE(COALESCE(unique_fraud_users,0),COALESCE(unique_users,0))*100) AS pct_unique_fraud_users
    ,days_since_last_bookable_offer
    ,COALESCE(partner_segmentation, "not activated") partner_segmentation
FROM `{{ bigquery_analytics_dataset }}`.enriched_cultural_partner_data
LEFT JOIN individual_offers_created ON enriched_cultural_partner_data.partner_id = individual_offers_created.partner_id
LEFT JOIN collective_offers_created ON enriched_cultural_partner_data.partner_id = collective_offers_created.partner_id
LEFT JOIN individual_bookings ON enriched_cultural_partner_data.partner_id = individual_bookings.partner_id
LEFT JOIN collective_bookings ON enriched_cultural_partner_data.partner_id = collective_bookings.partner_id
LEFT JOIN favorites ON enriched_cultural_partner_data.partner_id = favorites.partner_id
LEFT JOIN siren_status ON enriched_cultural_partner_data.partner_id = siren_status.partner_id
LEFT JOIN rejected_offers ON enriched_cultural_partner_data.partner_id = rejected_offers.partner_id
LEFT JOIN providers ON enriched_cultural_partner_data.partner_id = providers.partner_id
LEFT JOIN reimbursment_point ON enriched_cultural_partner_data.partner_id = reimbursment_point.partner_id
LEFT JOIN consultations ON enriched_cultural_partner_data.partner_id = consultations.partner_id
LEFT JOIN adage_status ON enriched_cultural_partner_data.partner_id = adage_status.partner_id
LEFT JOIN churn_segmentation ON enriched_cultural_partner_data.partner_id = churn_segmentation.partner_id