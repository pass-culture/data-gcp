WITH inapp_events AS (
    SELECT
      date(event_date) as event_date,
      CASE 
        WHEN app_id = "id1557887412" THEN "ios"
        WHEN app_id = "app.passculture.webapp" THEN "android"
        ELSE "unknown" END
      AS app,
      media_source as media_source,
      campaign as campaign,
      adset as adset,
      ad,
      sum(if(event_name = "af_conversion",  cast(event_count as float64), 0 )) as total_installs,
      sum(if(event_name = "af_complete_beneficiary", cast(unique_users as float64), 0 )) as total_beneficiaries,
      sum(if(event_name = "af_complete_registration", cast(unique_users as float64), 0 )) as total_registrations,
      sum(if(event_name = "af_open_app", cast(unique_users as float64), 0 )) as total_open_app,
      sum(if(event_name = "af_complete_book_offer", cast(unique_users as float64), 0 )) as total_bookings,
      
    FROM
        `{{ bigquery_appsflyer_import_dataset }}.cohort_user_acquisition` adr
    WHERE conversion_type = "install"
    GROUP BY 1,2,3,4,5,6
), 

global_stats AS (
  SELECT 
    date(dr.event_date) as event_date, 
    dr.app,
    if(dr.media_source = "Organic", "organic", dr.media_source) as media_source, 
    if(dr.campaign = "nan", null, dr.campaign) as campaign,
    if(dr.adset = "nan", null, dr.adset) as adset,
    if(dr.adgroup = "nan", null, dr.adgroup) as ad, 
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(installs) as installs, 
    sum(total_cost) as total_cost,
  
  FROM `{{ bigquery_raw_dataset }}.appsflyer_daily_report` dr
  GROUP BY 1,2,3,4,5,6
)

SELECT 
  ie.event_date, 
  ie.app,
  ie.media_source, 
  ie.campaign, 
  ie.adset, 
  ie.ad,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  sum(installs) as installs, 
  sum(total_cost) as total_cost,
  sum(coalesce(total_registrations, 0)) as total_registrations,
  sum(coalesce(total_beneficiaries, 0)) as total_beneficiaries,
  sum(coalesce(total_installs, 0)) as total_installs,
  sum(coalesce(total_open_app, 0)) as total_open_app,
  sum(coalesce(total_bookings, 0)) as total_bookings
FROM inapp_events ie 
LEFT JOIN global_stats dr on 
  date(dr.event_date) = ie.event_date AND
  dr.app = ie.app AND
  coalesce(dr.media_source, "-1") = coalesce(ie.media_source, "-1") AND
  coalesce(dr.campaign, "-1") = coalesce(ie.campaign, "-1") AND
  coalesce(dr.adset, "-1") = coalesce(ie.adset, "-1") AND
  coalesce(dr.ad, "-1") = coalesce(ie.ad, "-1")
GROUP BY 1,2,3,4,5,6
