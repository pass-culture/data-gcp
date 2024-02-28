{% set discovery_features = ['category_id', 'subcategory_id', 'new_consulted_item_id'] %}

WITH consultation as (
    SELECT 
        *
        , lag(consulted_items) over(partition by user_id order by consultation_date) as last_consulted_items
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
    FROM consultation
),
new_consultation_metadata as (
    SELECT
        user_id
        , consultation_date
        , new_items as new_consulted_item_id
        , category_id
        , subcategory_id
    FROM new_consultation, unnest(new_consulted_items) new_items
    LEFT JOIN {{ ref('item_metadata') }} metadata
    on new_items = metadata.item_id
)
SELECT 
    *
    , {% for feature in discovery_features %} 
        CASE
            WHEN consultation_date = min(consultation_date) over(partition by user_id, {{feature}})
            THEN 1
            ELSE 0
        END as {{feature}}_discovery
      {% if not loop.last -%} , {%- endif %}
      {% endfor %}
FROM new_consultation_metadata