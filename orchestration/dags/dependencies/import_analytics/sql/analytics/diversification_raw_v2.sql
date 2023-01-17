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
    , movie_type
    , type
  FROM `{{ bigquery_analytics_dataset }}.enriched_offer_data`
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
	-- création d'une extra catégorie pour observer la diversification en genre au sein d'une catégorie(style de musique, genre de film etc...)
    , CASE
        WHEN bookings.offer_category_id = "LIVRE" THEN rayon.macro_rayon
        WHEN bookings.offer_subcategoryId = "SEANCE_CINE" THEN movie_type
        WHEN bookings.offer_category_id = "MUSIQUE_LIVE" THEN type -- music type
        WHEN bookings.offer_category_id <> "MUSIQUE_LIVE" AND type IS NOT NULL then type -- show type
        ELSE venue_id
      END as extra_category
	-- attribuer un numéro de réservation
    , row_number() over(partition by users.user_id order by booking_creation_date) as booking_rank
FROM users
INNER JOIN bookings
  ON users.user_id = bookings.user_id
INNER JOIN offer
  ON bookings.offer_id = offer.offer_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.macro_rayons` rayon
  ON offer.rayon = rayon.rayon
),

diversification_scores as (
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
  -- Pour attribuer les scores de diversification : comparer la date de booking avec la première date de booking sur la feature correspondante.
  -- Lorsque ces 2 dates sont les mêmes, attribuer 1 point.
  , CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, category) AND booking_rank != 1
        THEN 1
        ELSE 0
    END as category_diversification
  , CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, subcategory) AND booking_rank != 1
        THEN 1 
        ELSE 0 
    END as sub_category_diversification
  , CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, format) AND booking_rank != 1
        THEN 1
        ELSE 0 END
    END as format_diversification
  , CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, venue_id) AND booking_rank != 1
        THEN 1 
        ELSE 0
    END as venue_diversification
  , CASE
      WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, extra_category) AND booking_rank != 1
      THEN 1 
      ELSE 0
    END as type_diversification
FROM base_diversification
)

SELECT
  user_id
  , booking_id
  , booking_creation_date
  , category_diversification
  , sub_category_diversification
  , format_diversification
  , venue_diversification
  , type_diversification
  , case 
      when booking_rank = 1 
      then 1  -- 1 point d'office pour le premier booking
      else category_diversification + sub_category_diversification + format_diversification + venue_diversification + type_diversification	
    end as delta_diversification
FROM diversification_scores