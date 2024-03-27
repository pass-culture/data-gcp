
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
    , bookings.venue_type_label
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

FROM {{ ref('enriched_booking_data') }} as bookings
WHERE booking_status != 'CANCELLED'
),
offer_metadata as (
  SELECT distinct
    offer_id
    , subcategory_id
    , category_id
    , offer_type_label
    , titelive_gtl_id
  FROM {{ ref('enriched_offer_metadata') }}
),
base_diversification as (
SELECT
    users.user_id
    , bookings.booking_creation_date
    , bookings.booking_id
    , bookings.item_id
    , bookings.venue_type_label
    , CASE
        WHEN subcategories.is_event = TRUE THEN "event"
        WHEN subcategories.online_offline_platform = "ONLINE" AND subcategories.is_event = FALSE THEN "digital"
        WHEN subcategories.online_offline_platform in ("OFFLINE", "ONLINE_OR_OFFLINE") AND subcategories.is_event = FALSE THEN "physical"
    END as format 
    , bookings.offer_id
    , is_free_offer
    , offer_metadata.category_id as category
    , offer_metadata.subcategory_id as sub_category
    , offer_metadata.titelive_gtl_id as titelive_gtl_id
    , item_metadata.cluster_id
    , item_metadata.topic_id
    , item_metadata.simple_cluster_id
    , item_metadata.simple_topic_id
    , item_metadata.unconstrained_cluster_id
    , item_metadata.unconstrained_topic_id
    -- prendre une venue unique pour les offres digitales
    , CASE
        WHEN bookings.digital_goods = True 
        THEN "digital_venue"
        ELSE venue_id
      END as venue_id
	-- création d'une extra catégorie pour observer la diversification en genre au sein d'une catégorie(style de musique, genre de film etc...)
    , CASE
        WHEN offer_metadata.offer_type_label IS NULL
        THEN venue_id
        ELSE offer_metadata.offer_type_label
      END as extra_category
  -- attribuer un numéro de réservation
    , row_number() over(partition by users.user_id order by booking_creation_date) as booking_rank

FROM users
INNER JOIN bookings
  ON users.user_id = bookings.user_id
LEFT JOIN offer_metadata
  ON bookings.offer_id = offer_metadata.offer_id
LEFT JOIN {{ source('clean','subcategories') }} subcategories
  ON offer_metadata.subcategory_id = subcategories.id
LEFT JOIN {{ ref('enriched_item_metadata') }} as item_metadata
  ON bookings.item_id = item_metadata.item_id

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
  , {% for feature in diversification_vars("diversification_features") %} 
  CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, {{feature}}) AND booking_rank != 1
        THEN 1
        ELSE 0
  END as {{feature}}_diversification
  {% if not loop.last -%} , {%- endif %}
  {% endfor %}
  , {% for feature in diversification_vars("diversification_features2") %} 
  CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, {{feature}}) AND booking_rank != 1
        THEN 1
        ELSE 0
  END as {{feature}}_diversification2
  {% if not loop.last -%} , {%- endif %}
  {% endfor %}
  , {% for feature in diversification_vars("diversification_features3") %} 
  CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, {{feature}}) AND booking_rank != 1
        THEN 1
        ELSE 0
  END as {{feature}}_diversification3
  {% if not loop.last -%} , {%- endif %}
  {% endfor %}
  , {% for feature in diversification_vars("diversification_features4") %} 
  CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, {{feature}}) AND booking_rank != 1
        THEN 1
        ELSE 0
  END as {{feature}}_diversification4
  {% if not loop.last -%} , {%- endif %}
  {% endfor %}
  , {% for feature in diversification_vars("diversification_features5") %} 
  CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, {{feature}}) AND booking_rank != 1
        THEN 1
        ELSE 0
  END as {{feature}}_diversification5
  {% if not loop.last -%} , {%- endif %}
  {% endfor %}
  , {% for feature in diversification_vars("diversification_features6") %} 
  CASE
        WHEN booking_creation_date = min(booking_creation_date) over(partition by user_id, {{feature}}) AND booking_rank != 1
        THEN 1
        ELSE 0
  END as {{feature}}_diversification6
  {% if not loop.last -%} , {%- endif %}
  {% endfor %}
FROM base_diversification
)

SELECT
  user_id
  , item_id
  , booking_id
  , booking_creation_date
  , {% for feature in diversification_vars("diversification_features") %}
    {{feature}}_diversification
    {% if not loop.last -%} , {%- endif %}
    {% endfor %}
  , {% for feature in diversification_vars("diversification_features2") %}
    {{feature}}_diversification2
    {% if not loop.last -%} , {%- endif %}
    {% endfor %}
  , {% for feature in diversification_vars("diversification_features3") %}
    {{feature}}_diversification3
    {% if not loop.last -%} , {%- endif %}
    {% endfor %}
  , {% for feature in diversification_vars("diversification_features4") %}
    {{feature}}_diversification4
    {% if not loop.last -%} , {%- endif %}
    {% endfor %}
  , case
      when booking_rank = 1 
        then 1 -- 1 point d'office pour le premier booking
      else -- somme des points de diversification pr les suivants
          {% for feature in diversification_vars("diversification_features") %} 
          {{feature}}_diversification 
          {% if not loop.last -%} + {%- endif %}
          {% endfor %}
    end as delta_diversification
  , case
      when booking_rank = 1 
        then 1 -- 1 point d'office pour le premier booking
      else -- somme des points de diversification pr les suivants
          {% for feature in diversification_vars("diversification_features2") %} 
          {{feature}}_diversification2
          {% if not loop.last -%} + {%- endif %}
          {% endfor %}
    end as delta_diversification2
  , case
      when booking_rank = 1 
        then 1 -- 1 point d'office pour le premier booking
      else -- somme des points de diversification pr les suivants
          {% for feature in diversification_vars("diversification_features3") %} 
          {{feature}}_diversification3
          {% if not loop.last -%} + {%- endif %}
          {% endfor %}
    end as delta_diversification3
  , case
      when booking_rank = 1 
        then 1 -- 1 point d'office pour le premier booking
      else -- somme des points de diversification pr les suivants
          {% for feature in diversification_vars("diversification_features4") %} 
          {{feature}}_diversification4
          {% if not loop.last -%} + {%- endif %}
          {% endfor %}
    end as delta_diversification4
  , case
      when booking_rank = 1 
        then 1 -- 1 point d'office pour le premier booking
      else -- somme des points de diversification pr les suivants
          {% for feature in diversification_vars("diversification_features5") %} 
          {{feature}}_diversification5
          {% if not loop.last -%} + {%- endif %}
          {% endfor %}
    end as delta_diversification5
  , case
      when booking_rank = 1 
        then 1 -- 1 point d'office pour le premier booking
      else -- somme des points de diversification pr les suivants
          {% for feature in diversification_vars("diversification_features6") %} 
          {{feature}}_diversification6
          {% if not loop.last -%} + {%- endif %}
          {% endfor %}
    end as delta_diversification6
FROM diversification_scores