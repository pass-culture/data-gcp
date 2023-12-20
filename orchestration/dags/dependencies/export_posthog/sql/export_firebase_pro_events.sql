SELECT  
    event_date, 
    CASE 
        WHEN event_name = "page_view" 
        THEN CONCAT("page_view"," ", ( select event_params.value.string_value from unnest(event_params) event_params where event_params.key = 'page_title' ))
    ELSE event_name 
    END as event_name,
    event_timestamp,
    firebase.user_id,
    user_pseudo_id,
    event_params,
    STRUCT(
        offerer.offerer_creation_date,
        offerer.offerer_first_offer_creation_date,
        offerer.offerer_first_individual_offer_creation_date,
        offerer.offerer_first_collective_offer_creation_date,
        offerer.legal_unit_business_activity_label,
        offerer.legal_unit_legal_category_label,
        offerer.is_synchro_adage,
        offerer.first_dms_adage_status,
        offerer.offerer_non_cancelled_individual_bookings,
        offerer.offerer_non_cancelled_collective_bookings,
        venue.venue_name,
        venue.venue_creation_date,
        venue.venue_type_label,
        venue.venue_is_permanent,
        venue.venue_is_virtual,
        CASE WHEN venue.venue_siret is not NULL THEN TRUE ELSE FALSE END AS venue_has_siret
    ) as extra_params, 
    STRUCT(
        user.user_creation_date,
        user.offerer_id
    ) as user_params,
    platform,
    app_info.version as app_version,
    'pro' as origin
FROM `{{ bigquery_clean_dataset }}.firebase_pro_events_{{ yyyymmdd(add_days(ds, params.days)) }}` firebase
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_offerer` user ON firebase.user_id=user.user_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_offerer_data` offerer ON user.offerer_id=offerer.offerer_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_venue_data` venue
ON venue.venue_id = (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'venueId'
        )
WHERE (NOT REGEXP_CONTAINS(event_name, '^[a-z]+(_[a-z]+)*$') OR event_name = "page_view")