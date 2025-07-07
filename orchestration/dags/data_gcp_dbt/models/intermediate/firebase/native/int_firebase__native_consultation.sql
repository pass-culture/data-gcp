{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "consultation_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

select distinct
    user_id,
    event_date as consultation_date,
    event_timestamp as consultation_timestamp,
    offer_id,
    origin,
    module_id,
    unique_session_id,
    event_name,
    venue_id,
    traffic_medium,
    traffic_campaign,
    traffic_source,
    search_query_input_is_generic,
    query,
    entry_id,
    home_type,
    similar_offer_id,
    similar_offer_playlist_type,
    multi_venue_offer_id,
    concat(user_id, "-", event_timestamp, "-", offer_id) as consultation_id
from {{ ref("int_firebase__native_event") }}
where
    event_name = "ConsultOffer" and user_id is not null and offer_id is not null
    {% if is_incremental() %}
        and date(event_date) >= date_sub('{{ ds() }}', interval 3 day)
    {% else %}
        and date(event_date)
        >= date_sub('{{ ds() }}', interval {{ var("full_refresh_lookback") }})
    {% endif %}
