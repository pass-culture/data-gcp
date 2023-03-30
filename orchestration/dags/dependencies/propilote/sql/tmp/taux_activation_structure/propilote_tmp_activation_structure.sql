WITH dates AS (
    SELECT 
        DISTINCT DATE_TRUNC(deposit_creation_date, MONTH) AS mois 
    FROM `{{ bigquery_analytics_dataset }}.enriched_deposit_data`
    WHERE deposit_creation_date >= '2023-01-01'
),

structures AS (
    SELECT DISTINCT
        venue_managing_offerer_id AS offerer_id
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'all' %}
            'NAT'
        {% else %}
            venue.{{ params.group_type_name }}
        {% endif %} as dimension_value
        , MIN(
            CASE
                WHEN venue_is_permanent
                THEN venue_creation_date
                ELSE NULL 
            END
        ) AS premier_lieu_permanent
        , COUNT(
            CASE
                WHEN (venue.venue_is_virtual AND offer_creation_date <= '2022-12-31') 
                THEN offer_id 
                ELSE NULL
            END
        ) AS offres_numeriques
    FROM `{{ bigquery_analytics_dataset }}.enriched_venue_data` as venue
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` as offer
        ON venue.venue_id = offer.venue_id 
        AND offer.venue_is_virtual 
    GROUP BY 1, 2, 3
    HAVING (premier_lieu_permanent < '2023-01-01' OR offres_numeriques > 0)
),

all_structures AS (
    SELECT 
        dimension_name
        , dimension_value
        , COUNT(DISTINCT offerer_id) AS nb_structures 
    FROM structures 
    GROUP BY 1, 2
),

active_individuel AS (
    SELECT DISTINCT 
        mois
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'all' %}
            'NAT'
        {% else %}
            venue.{{ params.group_type_name }}
        {% endif %} as dimension_value
        , offerer_id AS active_offerers
    FROM dates 
    JOIN `{{ bigquery_analytics_dataset }}.bookable_offer_history` as bookable_offer_history
        ON dates.mois >= DATE_TRUNC(bookable_offer_history.partition_date, MONTH)
    JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` as offer
        ON offer.offer_id = bookable_offer_history.offer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_venue_data` as venue
        ON venue.venue_id = offer.venue_id 
    WHERE offerer_id IN (SELECT DISTINCT offerer_id FROM structures)
),

active_collectif AS (
    SELECT DISTINCT 
        mois
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'all' %}
            'NAT'
        {% else %}
            collective_offer.{{ params.group_type_name }}
        {% endif %} as dimension_value
        , offerer_id AS active_offerers
    FROM dates 
    JOIN `{{ bigquery_analytics_dataset }}.bookable_collective_offer_history` as bcoh
        ON dates.mois >= DATE_TRUNC(bcoh.partition_date, MONTH)
    JOIN `{{ bigquery_analytics_dataset }}.enriched_collective_offer_data` as collective_offer
        ON collective_offer.collective_offer_id = bcoh.collective_offer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_venue_data` as venue
        ON venue.venue_id = collective_offer.venue_id 
    WHERE offerer_id IN (SELECT DISTINCT offerer_id FROM structures)
),

all_actives AS (
    SELECT * 
    FROM active_individuel
    UNION ALL 
    SELECT * 
    FROM active_collectif
),

count_actives AS (
    SELECT 
        mois
        , dimension_name
        , dimension_value
        , COUNT(DISTINCT active_offerers) AS active_offerers
    FROM all_actives 
    GROUP BY 1, 2, 3
)

SELECT 
    mois
    , all_structures.dimension_name
    , all_structures.dimension_value
    , NULL as user_type
    , "taux_activation_structure" as indicator
    , active_offerers as numerator
    , all_structures.nb_structures AS denominator
FROM all_structures 
CROSS JOIN count_actives 
