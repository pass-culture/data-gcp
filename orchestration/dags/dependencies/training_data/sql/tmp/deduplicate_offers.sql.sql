SELECT * except (deduplicated_offer_name) FROM
  (SELECT * FROM
    (SELECT * except (item_id) from `{{ bigquery_analytics_dataset }}`.`enriched_offer_data`) t1
  LEFT JOIN
    (SELECT offer_name as deduplicated_offer_name, MIN(item_id) as item_id FROM `{{ bigquery_analytics_dataset }}`.`enriched_offer_data` group by offer_name) t2
  ON
    t1.offer_name=t2.deduplicated_offer_name) order by offer_name