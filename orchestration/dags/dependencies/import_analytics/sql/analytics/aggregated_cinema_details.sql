with sources_cine as (
SELECT 
  boost_cinema_details_id as cinema_source_id , 
  'boost' as source ,
  cinema_url, 
  null as cinema_api_token, 
  null as account_id,
  cinema_provider_pivot_id
FROM `{{ bigquery_clean_dataset }}.applicative_database_boost_cinema_details`
UNION ALL
SELECT 
  cds_cinema_details_id as cinema_source_id ,
 'cds' as source,
  null as cinema_url,
  cinema_api_token,
  account_id,
  cinema_provider_pivot_id

FROM `{{ bigquery_clean_dataset }}.applicative_database_cds_cinema_details` 
UNION ALL
SELECT 
  cgr_cinema_details_id as cinema_source_id ,
 'cgr' as source,
  cinema_url,
  null as cinema_api_token, 
  null as account_id,
  cinema_provider_pivot_id
FROM `{{ bigquery_clean_dataset }}.applicative_database_cgr_cinema_details` 
)
select 
  sources_cine.* except(cinema_provider_pivot_id),
  cinepivot.* except(cinema_provider_pivot_id)
from sources_cine
left join `{{ bigquery_clean_dataset }}.applicative_database_cinema_provider_pivot` as cinepivot using(cinema_provider_pivot_id)