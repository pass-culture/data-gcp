WITH temp_firebase_events AS (
    SELECT
        event_name,
        user_pseudo_id,
        user_id,
        platform,
        PARSE_DATE("%Y%m%d", event_date) AS event_date,
        TIMESTAMP_SECONDS(
            CAST(CAST(event_timestamp as INT64) / 1000000 as INT64)
        ) AS event_timestamp,
        TIMESTAMP_SECONDS(
            CAST(CAST(event_timestamp as INT64) / 1000000 as INT64)
        ) AS user_first_touch_timestamp,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'ga_session_number'
        ) as session_number,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'ga_session_id'
        ) as session_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'venue_id'
        ) as venue_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'offerer_id'
        ) as offerer_humanized_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'page_title'
        ) as page_name,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'page_location'
        ) as page_location,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'page_referrer'
        ) as page_referrer,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'page_number'
        ) as page_number,
        (
            select
                CAST(event_params.value.double_value AS STRING)
            from
                unnest(event_params) event_params
            where
                event_params.key = 'offerId'
        ) as double_offer_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'offerId'
        ) as string_offer_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'from'
        ) as origin,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'to'
        ) as destination,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'used'
        ) as used,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'filled'
        ) as filled,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'filledWithErrors'
        ) as filledWithErrors,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'traffic_campaign'
        ) as traffic_campaign,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'traffic_medium'
        ) as traffic_medium,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'traffic_source'
        ) as traffic_source,
    FROM
        {% if params.dag_type == 'intraday' %}
        `{{ bigquery_clean_dataset }}.firebase_pro_events_{{ yyyymmdd(ds) }}`
        {% else %}
        `{{ bigquery_clean_dataset }}.firebase_pro_events_{{ yyyymmdd(add_days(ds, -1)) }}`
        {% endif %}

)
SELECT
    *
EXCEPT
(double_offer_id, string_offer_id),
    (
        CASE
            WHEN double_offer_id IS NULL THEN string_offer_id
            ELSE double_offer_id
        END
    ) AS offer_id
FROM
    temp_firebase_events