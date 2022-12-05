SELECT * EXCEPT (deduplicated_offer_name)
FROM (SELECT *
      FROM (SELECT * EXCEPT (item_id) FROM `{{ bigquery_analytics_dataset }}`.`enriched_offer_data`) t1
               LEFT JOIN
           (SELECT offer_name AS deduplicated_offer_name, MIN(item_id) AS item_id
            FROM `{{ bigquery_analytics_dataset }}`.`enriched_offer_data`
            GROUP BY offer_name) t2
           ON
               t1.offer_name = t2.deduplicated_offer_name)
ORDER BY offer_name