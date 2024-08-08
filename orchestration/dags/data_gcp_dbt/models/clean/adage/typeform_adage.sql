SELECT 
  case when date like '%/%/%' then PARSE_DATE("%d/%m/%Y",date)
  else date(date)
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
FROM {{ source('raw','gsheet_eac_webinar') }}