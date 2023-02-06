-- Get the most frequented iris by users over the last 6 months

WITH aggregated_reco as (SELECT
  userid as user_id
  , call_id
  , date as reco_date
  , user_iris_id
  , count(offerid) as nb_recommended_offers
FROM `{{ bigquery_clean_dataset }}.past_recommended_offers`
WHERE user_iris_id is not null AND user_iris_id != ""
GROUP BY
  userid
  , call_id
  , date
  , user_iris_id
),

count_iris as (SELECT
  user_id 
  , user_iris_id
  , count(user_iris_id) as iris_frequency_times
FROM aggregated_reco
WHERE DATE(reco_date) >= DATE_ADD(current_date(), INTERVAL - 6 MONTH)
GROUP BY
  user_id
  , user_iris_id
  ),

iris_frequency as (
SELECT
  user_id
  , user_iris_id as most_frequent_iris
  , iris_frequency_times 
  , row_number() over(partition by user_id order by iris_frequency_times desc) as rank
FROM count_iris
QUALIFY rank = 1
)

SELECT
  user_id
  , most_frequent_iris
  , iris_frequency_times
FROM iris_frequency

