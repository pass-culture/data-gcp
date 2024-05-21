SELECT 
  case when eac_webinar.date like '%/%/%' then PARSE_DATE("%d/%m/%Y",eac_webinar.date)
  else date(eac_webinar.date)
  end as date,
  first_name,
  last_name,
  email,
  registration_time,
  approval_status,
  offerer_name,
  job_title,
  siren,
  region_name,
  cultural_domain
FROM `{{ bigquery_raw_dataset }}.gsheet_eac_webinar` AS eac_webinar