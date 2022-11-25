WITH aggregated_reco_offers as (SELECT
  userid as user_id
  , call_id
  , date as reco_date
  , user_iris_id
  , count(offerid) as nb_recommended_offers
FROM `{{ bigquery_clean_dataset }}.past_recommended_offers`
WHERE user_iris_id is not null
group by 
  userid
  ,call_id
  ,date
  ,user_iris_id),

last_iris as (SELECT
  user_id
  , reco_date as last_reco_date
  , user_iris_id
  , row_number() over(partition by user_id order by reco_date desc) as rank
from aggregated_reco_offers
qualify rank = 1
)

SELECT 
  user_id
  , last_reco_date
  , user_iris_id
FROM last_iris


