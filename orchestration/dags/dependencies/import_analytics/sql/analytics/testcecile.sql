SELECT count(distinct partner_id) as nbr, venue_id FROM `{{ bigquery_analytics_dataset}}.enriched_cultural_partner_data` 
where venue_id != ""
group by venue_id 

LIMIT 1000
