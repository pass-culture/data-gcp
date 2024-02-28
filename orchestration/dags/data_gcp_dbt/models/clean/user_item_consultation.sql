SELECT 
    user.user_id
    , event_date as consultation_date
    , array_agg(distinct offer_item_ids.item_id) as consulted_item_id
FROM {{ ref('user_beneficiary') }} user
JOIN {{ source('analytics', 'firebase_events') }} firebase
    ON user.user_id = firebase.user_id
    and event_name = 'ConsultOffer'
    and event_date >= date_sub('{{ ds() }}', INTERVAL 3 day)
JOIN {{ ref('offer_item_ids') }} offer_item_ids
    ON firebase.offer_id = offer_item_ids.offer_id
GROUP BY 1, 2

with test as (SELECT *, lag(consulted_item_id) over(partition by user_id order by consultation_date) as last_consulted_item_id
FROM `passculture-data-prod.sandbox_prod.user_item_consultation`
where user_id = '3708739'
)
SELECT *, 
  ARRAY(SELECT * FROM test.consulted_item_id
    INTERSECT DISTINCT
    (SELECT * FROM test.last_consulted_item_id)
  ) AS result
FROM test
