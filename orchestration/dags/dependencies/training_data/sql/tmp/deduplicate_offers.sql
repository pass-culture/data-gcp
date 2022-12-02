SELECT * EXCEPT (deduplicated_offer_name)
FROM (SELECT *
      FROM (SELECT * EXCEPT (item_id)
            FROM `{{ bigquery_analytics_dataset }}`.`enriched_offer_data`
            WHERE offer_subcategoryId IN {{ event_subcategories }}) t1
               LEFT JOIN
           (SELECT offer_name AS deduplicated_offer_name, MIN(item_id) AS item_id
            FROM `{{ bigquery_analytics_dataset }}`.`enriched_offer_data`
            WHERE offer_subcategoryId IN {{ event_subcategories }}
            GROUP BY offer_name) t2
           ON
               t1.offer_name = t2.deduplicated_offer_name)
UNION ALL
SELECT * EXCEPT (item_id), item_id
FROM `{{ bigquery_analytics_dataset }}`.`enriched_offer_data`
WHERE offer_subcategoryId NOT IN {{ event_subcategories }}
ORDER BY offer_name