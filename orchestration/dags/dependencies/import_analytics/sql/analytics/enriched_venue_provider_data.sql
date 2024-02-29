SELECT 
  provider.provider_id
  , provider.provider_name
  , provider.is_active as is_active
  , venue_provider.venue_id as venue_id
  , venue.venue_name
  , venue_department_code
  , venue_creation_date
  , venue_is_permanent
  , venue_label
  , last_sync_date
  , count(distinct offer_id) as offer_sync_cnt
  , min(offer_creation_date) AS first_offer_sync_date
FROM `{{ bigquery_clean_dataset }}.applicative_database_provider` provider
JOIN `{{ bigquery_clean_dataset }}.applicative_database_venue_provider` venue_provider
USING(provider_id)
LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_venue` venue
USING(venue_id)
LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_venue_label` label
USING(venue_label_id)
LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_offer` offer
ON venue.venue_id = offer.venue_id
AND provider.provider_id = offer.offer_last_provider_id 
GROUP BY 
  provider.provider_id
  , provider.provider_name 
  , provider.is_active
  , venue_provider.venue_id
  , venue.venue_name
  , venue_department_code
  , venue_creation_date
  , venue_is_permanent
  , venue_label
  , last_sync_date