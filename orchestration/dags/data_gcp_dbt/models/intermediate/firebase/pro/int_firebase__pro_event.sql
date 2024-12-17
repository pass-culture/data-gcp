{{ config(pre_hook="{{ create_dehumanize_id_function() }}") }}

{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

with
    pro_event_raw_data as (
        select
            event_name,
            user_pseudo_id,
            case
                when regexp_contains(user_id, r"\D")
                then {{ target_schema }}.dehumanize_id(user_id)
                else user_id
            end as user_id,
            platform,
            event_date,
            event_timestamp,
            ga_session_number as session_number,
            ga_session_id as session_id,
            concat(user_pseudo_id, '-', ga_session_id) as unique_session_id,
            origin,
            destination,
            traffic_campaign,
            traffic_medium,
            traffic_source,
            category as user_device_category,
            operating_system as user_device_operating_system,
            operating_system_version as user_device_operating_system_version,
            browser as user_web_browser,
            browser_version as user_web_browser_version,
            coalesce(
                cast(offerer_id as string), cast(offererid as string)
            ) as offerer_id,
            coalesce(venue_id, cast(venueid as string)) as venue_id,
            page_location,
            page_title as page_name,
            regexp_extract(
                page_location, r"""passculture\.pro\/(.*)$""", 1
            ) as url_first_path,
            regexp_extract(
                page_location, r"""passculture\.pro\/(.*?)[\/.*?\/.*?\/|\?]""", 1
            ) as url_path_type,
            regexp_extract(
                page_location, r"""passculture\.pro\/.*?\/.*?\/(.*?)\/|\?""", 1
            ) as url_path_details,
            regexp_extract(
                page_location, r"""passculture\.pro\/(.*?)\?""", 1
            ) as url_path_before_params,
            regexp_extract_all(
                page_location, r'(?:\?|&)(?:([^=]+)=(?:[^&]*))'
            ) as url_params_key,
            regexp_extract_all(
                page_location, r'(?:\?|&)(?:(?:[^=]+)=([^&]*))'
            ) as url_params_value,
            regexp_replace(
                regexp_replace(page_location, "[A-Z \\d]+[\\?\\/\\&]?", ""),
                "https://passculture.pro/",
                ""
            ) as url_path_agg,
            page_referrer,
            page_number,
            CASE WHEN offer_type != 'collective' THEN coalesce(double_offer_id, offerid) ELSE NULL END as offer_id,
            CASE WHEN offer_type = 'collective' THEN offerId ELSE NULL END AS collective_offer_id
            offertype as offer_type,
            saved as has_saved_query,
            hasonly6eand5estudents as has_opened_wrong_student_modal,
            isedition as is_edition,
            isdraft as is_draft,
            filled,
            filledwitherrors as filled_with_errors,
            categoriejuridiqueunitelegale as onboarding_selected_legal_category,
            format as download_format,
            bookingstatus as download_booking_status,
            buttontype as download_button_type,
            filetype as download_file_type,
            filescount as download_files_cnt,
            subcategoryid as offer_subcategory_id,
            choosensuggestedsubcategory as suggested_offer_subcategory_selected,
            status AS collective_offer_status
        from {{ ref("int_firebase__pro_event_flattened") }}
        {% if is_incremental() %}
            where
                event_date
                between date_sub(date("{{ ds() }}"), interval 2 day) and date(
                    "{{ ds() }}"
                )
        {% endif %}
    )

select
    * except (url_first_path, url_path_type, url_path_details, url_path_before_params),
    case
        when url_path_details is null
        then replace(coalesce(url_path_before_params, url_first_path), "/", "-")
        when url_path_details is not null
        then concat(url_path_type, "-", url_path_details)
        else url_path_type
    end as url_path_extract
from pro_event_raw_data
