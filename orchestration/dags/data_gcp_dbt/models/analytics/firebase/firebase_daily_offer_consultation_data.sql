{{
    config(
        **custom_incremental_config(
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'event_date', 'data_type': 'date'},
    )
) }}

SELECT
  event_date,
  e.offer_id,
  offer_item_ids.item_id,
  origin,
  COUNT(*) AS nb_daily_consult
FROM
  {{ ref('int_firebase__native_event') }} e
LEFT JOIN
  {{ ref('offer_item_ids') }} offer_item_ids
ON
  offer_item_ids.offer_id = e.offer_id
WHERE
  event_name = 'ConsultOffer'
   {% if is_incremental() %} -- recalculate latest day's DATA + previous
AND
  DATE(event_date) >= DATE_SUB(DATE(_dbt_max_partition), INTERVAL 1 day)
{% endif %}
GROUP BY
  1,
  2,
  3,
  4
