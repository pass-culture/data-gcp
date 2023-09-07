{{ create_dehumanize_id_function() }}

WITH temp_firebase_events AS (
    SELECT
        event_name,
        user_pseudo_id,
        CASE WHEN REGEXP_CONTAINS(user_id, r"\D") THEN dehumanize_id(user_id) ELSE user_id END AS user_id,
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
        CONCAT(user_pseudo_id, '-',
                (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'ga_session_id'
                )
         ) AS unique_session_id,
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
                CAST(event_params.value.int_value AS STRING)
            from
                unnest(event_params) event_params
            where
                event_params.key = 'offerId'
        ) as int_offer_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'offerType'

        ) as offer_type,
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
                event_params.key = 'saved'
        ) as saved,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'hasOnly6eAnd5eStudents'
        ) as eac_wrong_student_modal_only6and5,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'isEdition'
        ) as is_edition,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'isDraft'
        ) as is_draft,
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
                event_params.key = 'BETTER_OFFER_CREATION'
        ) as is_new_offer_creation_path,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'categorieJuridiqueUniteLegale'
        ) as onboarding_selected_legal_category,
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
),


url_extract AS (
    SELECT
        * EXCEPT (double_offer_id, int_offer_id),
        (
            CASE
                WHEN double_offer_id IS NULL THEN int_offer_id
                ELSE double_offer_id
            END
        ) AS offer_id,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*)$""", 1) as url_first_path,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*?)[\/.*?\/.*?\/|\?]""", 1) as url_path_type,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/.*?\/.*?\/(.*?)\/|\?""", 1) as url_path_details,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*?)\?""", 1) as url_path_before_params,
        REGEXP_EXTRACT_ALL(page_location,r'(?:\?|&)(?:([^=]+)=(?:[^&]*))') as url_params_key,
        REGEXP_EXTRACT_ALL(page_location,r'(?:\?|&)(?:(?:[^=]+)=([^&]*))') as url_params_value,
        REGEXP_REPLACE(REGEXP_REPLACE(page_location , "[A-Z \\d]+[\\?\\/\\&]?", ""), "https://passculture.pro/", "") as url_path_agg
    FROM temp_firebase_events
)


SELECT
    * EXCEPT(url_first_path, url_path_type, url_path_details, url_path_before_params),
    CASE 
        WHEN url_path_details is NULL THEN REPLACE(COALESCE(url_path_before_params, url_first_path), "/", "-")
        WHEN url_path_details is NOT NULL THEN CONCAT(url_path_type, "-", url_path_details)
        ELSE url_path_type 
    END as url_path_extract
FROM
    url_extract