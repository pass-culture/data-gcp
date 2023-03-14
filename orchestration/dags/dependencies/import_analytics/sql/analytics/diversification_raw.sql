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
    , physical_goods
    , digital_goods
    , event
    , venue_id
FROM `{{ bigquery_analytics_dataset }}.enriched_booking_data`
WHERE booking_status != 'CANCELLED'
),
offer_metadata as (
  SELECT distinct
    offer_id
    , subcategory_id
    , category_id
    , offer_type_label
  FROM `{{ bigquery_analytics_dataset }}.enriched_offer_metadata`
),
base_diversification as (
SELECT
    users.user_id
    , bookings.booking_creation_date
    , bookings.booking_id
    , CASE
        WHEN subcategories.is_event = TRUE THEN "event"
        WHEN subcategories.online_offline_platform = "ONLINE" AND subcategories.is_event = FALSE THEN "digital"
        WHEN subcategories.online_offline_platform in ("OFFLINE", "ONLINE_OR_OFFLINE") AND subcategories.is_event = FALSE THEN "physical"
    END as format 
    , bookings.offer_id
    , is_free_offer
    , offer_metadata.category_id as category
    , offer_metadata.subcategory_id as sub_category
    -- prendre une venue unique pour les offres digitales
    , CASE
        WHEN bookings.digital_goods = True 
        THEN "digital_venue"
        ELSE venue_id
      END as venue_id
	-- création d'une extra catégorie pour observer la diversification en genre au sein d'une catégorie(style de musique, genre de film etc...)
    , CASE
        WHEN offer_type_label IS NULL
        THEN venue_id
        ELSE offer_type_label
      END as extra_category
  -- attribuer un numéro de réservation
    , row_number() over(partition by users.user_id order by booking_creation_date) as booking_rank
FROM users
INNER JOIN bookings
  ON users.user_id = bookings.user_id
LEFT JOIN offer_metadata
  ON bookings.offer_id = offer_metadata.offer_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.subcategories` subcategories
  ON offer_metadata.subcategory_id = subcategories.id
),

diversification_scores as (
  SELECT
  user_id
  , booking_id
  , booking_rank
  , booking_creation_date
  , is_free_offer
  , category
  , sub_category
  , format
  , extra_category
  -- Pour attribuer les scores de diversification : 
  -- Comparer la date de booking avec la première date de booking sur chaque feature.
  -- Lorsque ces 2 dates sont les mêmes, attribuer 1 point.
  , {% for feature in params.diversification_features %} 
  CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, {{feature}}) AND booking_rank != 1
        THEN 1
        ELSE 0
  END as {{feature}}_diversification
  {% if not loop.last -%} , {%- endif %}
  {% endfor %}
FROM base_diversification
)

SELECT
  user_id
  , booking_id
  , booking_creation_date
  , category_diversification
  , sub_category_diversification
  , format_diversification
  , venue_id_diversification
  , extra_category_diversification
  , case
      when booking_rank = 1 
      then 1 -- 1 point d'office pour le premier booking
      else category_diversification + sub_category_diversification + format_diversification + venue_id_diversification + extra_category_diversification	
    end as delta_diversification
FROM diversification_scores