WITH ir as (SELECT
start_date
, end_date
, response_id
, venue_id
, user_type
, question
, answer
, execution_date
, topics
, anciennete_jours
FROM `{{ bigquery_analytics_dataset }}.qualtrics_answers_ir_pro`
)

, ir_per_user as (
SELECT *
FROM ir
PIVOT
(
  min(answer)
  FOR question in ('Q1', 'Q2')
))

, indiv_book as (
SELECT 
  response_id,
  booking.venue_id,
  count(distinct booking_id) individual_bookings,
  max(booking_creation_date) last_individual_booking
FROM `{{ bigquery_clean_dataset }}.applicative_database_booking` AS booking
JOIN ir_per_user on ir_per_user.venue_id = booking.venue_id AND booking.booking_creation_date <= DATE(ir_per_user.start_date)
GROUP BY 1,2
)

, collective_book as (
SELECT 
  response_id,
  collective_booking.venue_id,
  count(distinct collective_booking_id) collective_bookings,
  max(collective_booking_creation_date) last_collective_booking
FROM `{{ bigquery_clean_dataset }}.applicative_database_collective_booking` AS collective_booking
JOIN ir_per_user on ir_per_user.venue_id = collective_booking.venue_id AND collective_booking.collective_booking_creation_date <= DATE(ir_per_user.start_date)
GROUP BY 1,2
ORDER BY 3 
)

, indiv_offer as (
SELECT
  response_id,
  offer.venue_id,
  count(distinct offer_id) individual_offers_created 
FROM `{{ bigquery_clean_dataset }}.applicative_database_offer` AS offer
JOIN ir_per_user on ir_per_user.venue_id = offer.venue_id AND offer.offer_creation_date <= DATE(ir_per_user.start_date)
GROUP BY 1, 2
)

, collective_offer as (
SELECT
  response_id,
  collective_offer.venue_id,
  count(distinct collective_offer_id) collective_offers_created 
FROM `{{ bigquery_clean_dataset }}.applicative_database_collective_offer` AS collective_offer
JOIN ir_per_user on ir_per_user.venue_id = collective_offer.venue_id AND collective_offer.collective_offer_creation_date <= DATE(ir_per_user.start_date)
GROUP BY 1, 2
)

, first_dms as (
SELECT 
  global_venue.venue_id,
  dms_pro.* 
FROM {{ bigquery_analytics_dataset }}.dms_pro 
LEFT JOIN {{ bigquery_analytics_dataset }}.global_venue on dms_pro.demandeur_siret = global_venue.venue_siret
WHERE procedure_id IN ('57081', '57189','61589','65028','80264')
AND demandeur_siret <> "nan"
AND venue_id is not null 
QUALIFY row_number() OVER(PARTITION BY demandeur_siret ORDER BY application_submitted_at ASC) = 1
)

, first_dms_adage as (
SELECT 
    ir_per_user.venue_id,
    ir_per_user.response_id,
    CASE WHEN first_dms.demandeur_siret IS NULL THEN "dms_adage_non_depose"
    WHEN first_dms.processed_at < timestamp(start_date) THEN application_status
    WHEN first_dms.passed_in_instruction_at < timestamp(start_date) THEN "en_instruction"
    WHEN first_dms.application_submitted_at < timestamp(start_date) THEN "en_construction"
    ELSE "dms_adage_non_depose" END as dms_adage_application_status
FROM ir_per_user 
LEFT JOIN first_dms on ir_per_user.venue_id = first_dms.venue_id 
    
)


SELECT 
  CASE WHEN EXTRACT(DAY from DATE(start_date)) >= 16 THEN DATE_ADD(DATE_TRUNC(DATE(start_date),MONTH), INTERVAL 1 MONTH) ELSE DATE_TRUNC(DATE(start_date), MONTH) END AS mois_releve,
  ir_per_user.venue_id,
  ir_per_user.response_id,
  SAFE_CAST(ir_per_user.Q1 as int) as note,
  ir_per_user.topics,
  CASE
    WHEN SAFE_CAST(ir_per_user.Q1 as int) <= 6 THEN "detracteur"
    WHEN SAFE_CAST(ir_per_user.Q1 as int) < 9 THEN "neutre"
    ELSE "promoteur"
  END as user_satisfaction,
  ir_per_user.Q2 AS commentaire,
  CASE
  WHEN (
    lower(ir_per_user.topics) like "%inscription longue eac%"
  OR lower(ir_per_user.topics) like "%référencement adage%"
  ) THEN TRUE
  ELSE FALSE
  END as mauvaise_exp_adage,
  CASE WHEN venue.venue_is_permanent THEN CONCAT("venue-",venue.venue_id)
         ELSE CONCAT("offerer-", offerer.offerer_id) END AS partner_id,
  venue.venue_type_code AS venue_type_label,
  venue_label.venue_label AS venue_label,
  venue.venue_department_code,
  IFNULL(indiv_book.individual_bookings, 0) individual_bookings,
  indiv_book.last_individual_booking,
  IFNULL(collective_book.collective_bookings, 0) collective_bookings,
  collective_book.last_collective_booking,
  IFNULL(indiv_book.individual_bookings, 0) + IFNULL(collective_book.collective_bookings, 0) total_bookings,
  IFNULL(indiv_offer.individual_offers_created, 0) individual_offers_created,
  IFNULL(collective_offer.collective_offers_created, 0) collective_offers_created,
  IFNULL(indiv_offer.individual_offers_created, 0) + IFNULL(collective_offer.collective_offers_created, 0) total_offers_created,
  IFNULL(dms_adage_application_status, "dms_adage_non_depose") dms_adage_application_status
FROM ir_per_user
LEFT JOIN indiv_book on ir_per_user.response_id = indiv_book.response_id
LEFT JOIN collective_book on ir_per_user.response_id = collective_book.response_id
LEFT JOIN indiv_offer on ir_per_user.response_id = indiv_offer.response_id 
LEFT JOIN collective_offer on ir_per_user.response_id = collective_offer.response_id 
LEFT JOIN first_dms_adage on ir_per_user.venue_id = first_dms_adage.venue_id AND ir_per_user.response_id = first_dms_adage.response_id
LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_venue` venue on ir_per_user.venue_id = venue.venue_id 
LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_offerer` AS offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_venue_label` AS venue_label ON venue_label.venue_label_id = venue.venue_label_id