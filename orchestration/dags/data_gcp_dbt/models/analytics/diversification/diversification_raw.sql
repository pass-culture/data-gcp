
WITH users AS (
  SELECT DISTINCT 
    user_id
  FROM {{ ref('user') }}
  ),
bookings AS (
  SELECT 
    bookings.user_id
    , bookings.item_id
    , bookings.offer_id
    , bookings.booking_amount
    , CASE
        WHEN bookings.booking_amount = 0
        THEN 1
        ELSE 0
      END as is_free_offer
    , bookings.booking_creation_date
    , bookings.booking_id
    , bookings.physical_goods
    , bookings.digital_goods
    , bookings.event
    , bookings.venue_id
    , ictl.category as clustering_category
    , ictl.semantic_category as clustering_semmantic_category
    , ictl.semantic_cluster_id
    , ictl.topic_id
    , ictl.x_cluster
    , ictl.y_cluster

FROM {{ ref('enriched_booking_data') }} as bookings
LEFT JOIN {{ ref('item_clusters_topics_labels') }} as ictl on ictl.item_id = bookings.item_id
WHERE booking_status != 'CANCELLED'
),
offer_metadata as (
  SELECT distinct
    offer_id
    , subcategory_id
    , category_id
    , offer_type_label
    , offer_type_domain
    , gtl_type
    , titelive_gtl_id
    , gtl_label_level_1
    , gtl_label_level_2
    , gtl_label_level_3
    , gtl_label_level_4
  FROM {{ ref('offer_metadata') }}
),
base_diversification as (
SELECT
    users.user_id
    , bookings.booking_creation_date
    , bookings.booking_id
    , bookings.item_id
    , CASE
        WHEN subcategories.is_event = TRUE THEN "event"
        WHEN subcategories.online_offline_platform = "ONLINE" AND subcategories.is_event = FALSE THEN "digital"
        WHEN subcategories.online_offline_platform in ("OFFLINE", "ONLINE_OR_OFFLINE") AND subcategories.is_event = FALSE THEN "physical"
    END as format 
    , bookings.offer_id
    , is_free_offer
    , offer_metadata.category_id as category
    , offer_metadata.subcategory_id as sub_category
    , offer_metadata.offer_type_domain
    , offer_metadata.gtl_type
    , offer_metadata.titelive_gtl_id
    , offer_metadata.gtl_label_level_1
    , offer_metadata.gtl_label_level_2
    , offer_metadata.gtl_label_level_3
    , offer_metadata.gtl_label_level_4
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
  -- features de clustering
    , ictl.micro_category_details
    , ictl.macro_category_details
    , ictl.semantic_category
    , ictl.category_lvl0
    , ictl.category_lvl1
    , ictl.category_lvl2
    , ictl.category_genre_lvl1
    , ictl.category_genre_lvl2
    , ictl.category_medium_lvl1
    , ictl.category_medium_lvl2
    , ictl.semantic_cluster_id
FROM users
INNER JOIN bookings
  ON users.user_id = bookings.user_id
LEFT JOIN offer_metadata
  ON bookings.offer_id = offer_metadata.offer_id
LEFT JOIN {{ source('clean','subcategories') }} subcategories
  ON offer_metadata.subcategory_id = subcategories.id
LEFT JOIN {{ref('item_clusters_topics_labels') }} as ictl
  ON bookings.item_id = ictl.item_id


),

diversification_scores as (
  SELECT
  user_id
  , booking_id
  , item_id
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
  , {% for feature in ml_vars("diversification_features") %} 
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
  , item_id
  , booking_id
  , booking_creation_date
  , {% for feature in ml_vars("diversification_features") %}
    {{feature}}_diversification
    {% if not loop.last -%} , {%- endif %}
    {% endfor %}
  , case
      when booking_rank = 1 then 1 -- 1 point d'office pour le premier booking
      else -- somme des points de diversification pr les suivants
          {% for feature in ml_vars("diversification_features") %} 
          {{feature}}_diversification 
          {% if not loop.last -%} + {%- endif %}
          {% endfor %}
    end as delta_diversification
FROM diversification_scores
