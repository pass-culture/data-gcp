SELECT 
    institutional_offerer_id
    ,zendesk_id
    ,institutional_offerer_name
    ,city
    ,region_name
    ,institutional_offerer_type
    ,related_team_pass
FROM `{{ bigquery_raw_dataset }}.institutional_partners`