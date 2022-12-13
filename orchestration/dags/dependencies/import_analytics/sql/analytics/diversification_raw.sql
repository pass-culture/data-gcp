WITH users AS (
  SELECT DISTINCT 
    user_id
  FROM `{{ bigquery_clean_dataset }}.applicative_database_user`
  ),
bookings AS (
  SELECT 
    user_id
    , offer_id
    , booking_amount
    , CASE
        WHEN booking_amount = 0
        THEN 1
        ELSE 0
      END as is_free_offer
    , booking_creation_date
    , booking_id
    , offer_subcategoryId
    , physical_goods
    , digital_goods
    , event
    , offer_category_id
FROM `{{ bigquery_analytics_dataset }}.enriched_booking_data`
WHERE booking_status != 'CANCELLED'
),
offer AS (
  SELECT distinct
    offer_id
    , rayon
    , venue_id
  FROM `{{ bigquery_analytics_dataset }}.enriched_offer_data`
),
qpi_answers AS (
  SELECT 
    user_id,
    ARRAY_AGG(STRUCT(subcategory_id) ORDER BY subcategory_id DESC) as qpi_subcategories  
  FROM (SELECT user_id, subcategory_id FROM `{{ bigquery_analytics_dataset }}.enriched_aggregated_qpi_answers`) as qpi 
  GROUP BY user_id
  ),
base_diversification as (
SELECT
    users.user_id
    , bookings.booking_creation_date
    , bookings.booking_id
    , COALESCE(
      IF(bookings.physical_goods = True, 'physical', null),
      IF(bookings.digital_goods = True, 'digital', null),
      IF(bookings.event = True, 'event', null)
    ) as format
    , offer.offer_id
    , is_free_offer
    , bookings.offer_category_id as category
    , bookings.offer_subcategoryId as subcategory
    , offer.rayon
    , rayon.macro_rayon
    -- prendre une venue unique pour les offres digitales
    , CASE
        WHEN bookings.digital_goods = True 
        THEN "digital_venue"
        ELSE offer.venue_id
      END as venue_id
		-- création d'une extra catégorie pour compenser le macro rayon des livres
    , CASE
        WHEN bookings.offer_category_id = "LIVRE"
        THEN rayon.macro_rayon
        ELSE venue_id
      END as extra_category
    , qpi_answers.qpi_subcategories as qpi_subcategories
		-- attribuer un numéro de réservation
    , row_number() over(partition by users.user_id order by booking_creation_date) as booking_rank
FROM users
INNER JOIN bookings
  ON users.user_id = bookings.user_id
INNER JOIN offer
  ON bookings.offer_id = offer.offer_id
LEFT JOIN qpi_answers
  ON users.user_id = qpi_answers.user_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.macro_rayons` rayon
  ON offer.rayon = rayon.rayon
),

diversification_scores as (
  -- ajouter la dimension des catégories renseignées dans le qpi pour les confronter aux catégories des bookings réalisés
  SELECT
  user_id
  , booking_id
  , booking_rank
  , booking_creation_date
  , is_free_offer
  , category
  , subcategory
  , format
  , macro_rayon
  , extra_category
  , subcategory_id as qpi_subcategory
  -- Pour attribuer les scores de diversification : comparer la date de booking avec la première date de booking sur la feature correspondante.
  -- Lorsque ces 2 dates sont les mêmes, attribuer 1 si offre payante, 0.5 si offre gratuite.
	, CASE
      WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, category) AND booking_rank != 1
      THEN 
        CASE
          WHEN is_free_offer = 1 
          THEN 0.5 
          ELSE 1 END
      ELSE 
        CASE
          -- Compléter le score par 0.5 si la catégorie a déjà été booké via une offre gratuite
          WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, category, is_free_offer) AND booking_rank != 1 
          THEN 0.5 
          ELSE 0 END
    END as category_diversification
  , CASE
      WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, subcategory) AND booking_rank != 1
      THEN 
        CASE
          WHEN is_free_offer = 1 
          THEN 0.5 
          ELSE 1 END
      ELSE 
        CASE
          -- Compléter le score par 0.5 si la sous-catégorie a déjà été booké via une offre gratuite
          WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, subcategory, is_free_offer) AND booking_rank != 1 
          THEN 0.5 
          ELSE 0 END
    END as sub_category_diversification
  , CASE
      WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, format) AND booking_rank != 1
      THEN 
        CASE
          WHEN is_free_offer = 1 
          THEN 0.5 
          ELSE 1 END
      ELSE 
        CASE
          -- Compléter le score par 0.5 si le format a déjà été booké via une offre gratuite
          WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, format, is_free_offer) AND booking_rank != 1 
          THEN 0.5 
          ELSE 0 END
    END as format_diversification
  , CASE
      WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, venue_id) AND booking_rank != 1
      THEN 
        CASE
          WHEN is_free_offer = 1 
          THEN 0.5 
          ELSE 1 END
      ELSE 
        CASE
          -- Compléter le score par 0.5 si la venue a déjà été booké via une offre gratuite
          WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, venue_id, is_free_offer) AND booking_rank != 1 
          THEN 0.5 
          ELSE 0 END
    END as venue_diversification
  , CASE
      WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, extra_category) AND booking_rank != 1
      THEN 
        CASE
          WHEN is_free_offer = 1 
          THEN 0.5 
          ELSE 1 END
      ELSE 
        CASE
          -- Compléter le score par 0.5 si l'extra catégorie a déjà été booké via une offre gratuite
          WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, extra_category, is_free_offer) AND booking_rank != 1 
          THEN 0.5 
          ELSE 0 END
    END as macro_rayon_diversification
  , CASE
      WHEN subcategory = subcategory_id or subcategory_id is null
      THEN 0
      ELSE 1
    END as is_diversified_booking_from_qpi
FROM base_diversification
LEFT JOIN unnest(qpi_subcategories) as qpi_subcategories
), 

diversification_scores_qpi as(
  -- aggréger par booking, utilisateur (retirer la dimension des sous-catégories renseignés dans le qpi)
SELECT 
  user_id
  , booking_id
  , booking_rank
  , is_free_offer
  , category
  , subcategory
  , format 
  , booking_creation_date
  , category_diversification
  , sub_category_diversification
  , format_diversification
  , venue_diversification
  , macro_rayon_diversification
  , min(is_diversified_booking_from_qpi) as is_diversified_booking_from_qpi
FROM diversification_scores
GROUP BY 
  user_id
  , booking_id
  , booking_rank
  , is_free_offer
  , category
  , subcategory
  , format 
  , booking_creation_date
  , category_diversification
  , sub_category_diversification
  , format_diversification
  , venue_diversification
  , macro_rayon_diversification),

first_diversification_from_qpi as (
SELECT
  diversification_scores_qpi.*
  -- attribuer un numéro de booking parmi les réservations diversifiantes par rapport au qpi pour récupérér uniquement le 1er
  , row_number() over(partition by user_id, is_diversified_booking_from_qpi order by booking_creation_date) as qpi_diversification_time_rank
FROM diversification_scores_qpi),

diversification_booking as (
  -- récupérer le premier booking diversifiant par rapport au qpi 
SELECT
  first_diversification_from_qpi.*
   , case 
      when is_diversified_booking_from_qpi = 1 and qpi_diversification_time_rank = 1
      then case when is_free_offer = 1 then 0.5 else 1 end  
      else 0 
    end as qpi_diversification
FROM first_diversification_from_qpi
)

SELECT
  user_id
  , booking_id
  , booking_creation_date
  , category_diversification
  , sub_category_diversification
  , format_diversification
  , venue_diversification
  , macro_rayon_diversification
  , qpi_diversification
  , case 
      when booking_rank = 1 
      then 1 + qpi_diversification -- 1 point d'office + les points du qpi pour la première réservation 
      else qpi_diversification + category_diversification	+ sub_category_diversification + format_diversification + venue_diversification + macro_rayon_diversification	
    end as delta_diversification
FROM diversification_booking