SELECT
    *
    , DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date
FROM `{{ bigquery_analytics_dataset }}`.`enriched_venue_tags_data`