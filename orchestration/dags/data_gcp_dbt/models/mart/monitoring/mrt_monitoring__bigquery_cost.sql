SELECT
  gbc.start_date,
  gbc.creation_date,
  coalesce(mu.email, gbc.user_email) as user_email,
  CASE
    WHEN mu.email is not null THEN 'Metabase'
    WHEN gbc.user_email like '%matabase%' THEN 'Metabase'
    WHEN gbc.user_email like '%composer%' THEN 'Composer'
    ELSE 'Bigquery (adhoc)'
    END
  as origin,
  gbc.dataset_id,
  gbc.table_id,
  mc.card_name,
  mc.dashboard_name,
  mc.card_id,
  mc.card_id_last_editor_email,
  mc.card_id_last_edition_date,
  mc.dashboard_id,
  sum(gbc.cost_euro) as cost_euro,
  sum(gbc.total_gigabytes_billed) as total_gigabytes,
  sum(gbc.total_bytes_billed)  as total_bytes,
  sum(gbc.total_queries) as total_queries
FROM {{ ref('int_gcp__bigquery_cost') }} gbc
LEFT JOIN {{ ref('int_metabase__user') }} mu on mu.user_id = gbc.metabase_user_id
LEFT JOIN {{ ref('mrt_monitoring__metabase_cost') }} mc on
  (date(mc.date) = date(gbc.creation_date)
  and mc.metabase_hash = gbc.metabase_hash
  and mc.metabase_user_id = gbc.metabase_user_id)

WHERE NOT cache_hit
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
