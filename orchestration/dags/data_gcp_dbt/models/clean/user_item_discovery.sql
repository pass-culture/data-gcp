WITH consultation as (
    SELECT 
        *
        , lag(consulted_items) over(partition by user_id order by consultation_date) as last_consulted_items
        , lag(consulted_origins) over(partition by user_id order by consultation_date) as last_consulted_origins
    FROM {{ ref('user_item_consultation') }}
),
new_consultation as (
    SELECT 
        user_id
        , consultation_date 
        , ARRAY(
            SELECT * FROM consultation.consulted_items
            EXCEPT DISTINCT
            (SELECT * FROM consultation.last_consulted_items)
        ) AS new_consulted_items
        , ARRAY(
            SELECT * FROM consultation.consulted_origins
            EXCEPT DISTINCT
            (SELECT * FROM consultation.last_consulted_origins)
        ) AS new_consulted_origins
    FROM consultation
),
new_consultation_item as (
    SELECT
        user_id
        , consultation_date
        , new_items
        , category_id
        , subcategory_id
        , {% for feature in discovery_vars("discovery_features") %} 
        CASE
            WHEN consultation_date = min(consultation_date) over(partition by user_id, {{feature}})
            THEN 1
            ELSE 0
        END as {{feature}}_discovery
      {% if not loop.last -%} , {%- endif %}
      {% endfor %}
    FROM new_consultation, unnest(new_consulted_items) new_items
    LEFT JOIN {{ ref('item_metadata') }} metadata
    on new_items = metadata.item_id
),
new_consultation_origin as (
    SELECT
        user_id
        , consultation_date
        , new_origins as new_consulted_origin
        , CASE
            WHEN consultation_date = min(consultation_date) over(partition by user_id, new_origins)
            THEN 1
            ELSE 0
        END as new_origins_discovery
    FROM new_consultation, unnest(new_consulted_origins) as new_origins
),

agg_new_consultation_origin as (
    SELECT 
        user_id
        , consultation_date
        , sum(new_origins_discovery) AS discovery_origin
    FROM new_consultation_origin
    GROUP BY 1,2
),

agg_new_consultation_item as (
        SELECT
        user_id
        , consultation_date  
        , {% for feature in discovery_vars("discovery_features") %} 
        sum({{feature}}_discovery) as discovery_{{feature}}
      {% if not loop.last -%} , {%- endif %}
      {% endfor %}
    FROM new_consultation_item
    GROUP BY 1, 2
)

SELECT *
FROM agg_new_consultation_item
FULL OUTER JOIN agg_new_consultation_origin
USING (user_id, consultation_date)
