SELECT
    venue_criterion.venue_id
    , venue_criterion.criterion_id
    , criterion_category_mapping.criterion_category_id
    , criterion_category.criterion_category_label
    , criterion.name criterion_name
FROM `{{ bigquery_clean_dataset }}`.applicative_database_venue_criterion AS venue_criterion
JOIN `{{ bigquery_clean_dataset }}`.applicative_database_criterion_category_mapping AS criterion_category_mapping
    USING(criterion_id)
JOIN `{{ bigquery_clean_dataset }}`.applicative_database_criterion_category AS criterion_category
    USING(criterion_category_id)
JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_criterion AS criterion ON criterion_category_mapping.criterion_id = criterion.id