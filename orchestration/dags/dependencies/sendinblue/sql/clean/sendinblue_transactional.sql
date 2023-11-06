with user_infos as (
  SELECT 
    tag
    , template
    , email
    , sum(delivered_count) as delivered_count
    , sum(opened_count) as opened_count
    , sum(unsubscribed_count) as unsubscribed_count
  FROM `{{ bigquery_raw_dataset }}.sendinblue_transactional_detailed`
  GROUP BY 
    tag
    , template
    , email
),

infos_aggregated as (
SELECT
tag
, template
, sum(delivered_count) as delivered_count
, sum(opened_count) as opened_count
, sum(case when opened_count > 0 then 1 else 0 end) as unique_opened_count
, sum(unsubscribed_count) as unsubscribed_count
FROM user_infos
GROUP BY 
tag
, template
)

SELECT 
    tag
    , template
    , delivered_count
    , opened_count
    , unique_opened_count
    , unsubscribed_count
    , DATE('{{ ds }}')  as update_date
FROM infos_aggregated
