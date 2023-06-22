WITH dates AS (
    SELECT 
        DISTINCT DATE_TRUNC(deposit_creation_date, MONTH) AS month 
    FROM `{{ bigquery_analytics_dataset }}.enriched_deposit_data`
    WHERE deposit_creation_date >= '2023-01-01'
),

structures AS (
    SELECT DISTINCT
        venue_managing_offerer_id AS offerer_id
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            rd.{{ params.group_type_name }}
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
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_offerer_data` as offerer 
        ON venue.venue_managing_offerer_id = offerer.offerer_id 
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        ON  offerer.offerer_department_code = rd.num_dep 
    GROUP BY 1, 2, 3
    HAVING (premier_lieu_permanent < '2023-01-01' OR offres_numeriques > 0)
),

all_structures AS (
    SELECT 
        month
        , dimension_name
        , dimension_value
        , COUNT(DISTINCT offerer_id) AS nb_structures 
    FROM structures 
    CROSS JOIN dates
    GROUP BY 1, 2, 3
),


active_individuel AS (
    SELECT DISTINCT 
        month
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            rd.{{ params.group_type_name }}
        {% endif %} as dimension_value
        , offerer_id AS active_offerers
    FROM dates 
    JOIN `{{ bigquery_analytics_dataset }}.bookable_offer_history` as bookable_offer_history
        ON dates.month >= DATE_TRUNC(bookable_offer_history.partition_date, MONTH)
    JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` as offer
        ON offer.offer_id = bookable_offer_history.offer_id
    JOIN `{{ bigquery_analytics_dataset }}.enriched_venue_data` as venue
        ON venue.venue_id = offer.venue_id 
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  venue.venue_department_code = rd.num_dep 
),

active_collectif AS (
    SELECT DISTINCT 
        month
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            rd.{{ params.group_type_name }}
        {% endif %} as dimension_value
        , offerer_id AS active_offerers
    FROM dates 
    JOIN `{{ bigquery_analytics_dataset }}.bookable_collective_offer_history` as bcoh
        ON dates.month >= DATE_TRUNC(bcoh.partition_date, MONTH)
    JOIN `{{ bigquery_analytics_dataset }}.enriched_collective_offer_data` as collective_offer
        ON collective_offer.collective_offer_id = bcoh.collective_offer_id
    JOIN `{{ bigquery_analytics_dataset }}.enriched_venue_data` as venue
        ON venue.venue_id = collective_offer.venue_id 
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  venue.venue_department_code = rd.num_dep 
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
       all_actives.month
        , all_actives.dimension_name
        , all_actives.dimension_value
        , COUNT(DISTINCT all_actives.active_offerers) AS active_offerers
    FROM all_actives 
    -- and already in past structures
    JOIN structures on 
    structures.offerer_id = all_actives.active_offerers
    AND structures.dimension_name = all_actives.dimension_name 
    AND structures.dimension_value = all_actives.dimension_value 
    GROUP BY 1, 2, 3
)


SELECT 
    all_structures.month
    , all_structures.dimension_name
    , all_structures.dimension_value
    , NULL as user_type
    , "taux_activation_structure" as indicator
    , COALESCE(count_actives.active_offerers, 0) as numerator
    , COALESCE(all_structures.nb_structures, 0) AS denominator
FROM all_structures  
LEFT JOIN count_actives 
  on all_structures.dimension_name = count_actives.dimension_name
  AND all_structures.dimension_value = count_actives.dimension_value
  AND all_structures.month = count_actives.month