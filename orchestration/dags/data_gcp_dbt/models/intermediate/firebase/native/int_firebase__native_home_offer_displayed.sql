{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            cluster_by=["module_id", "user_id", "unique_session_id", "offer_id"],
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

select
    native_event.event_date,
    offer_id,
    native_event.module_id,
    native_event.entry_id,
    native_event.user_id,
    native_event.unique_session_id,
    native_event.event_timestamp,
    position + 1 as displayed_position  -- noqa: RF01
from
    {{ ref("int_firebase__native_event") }} as native_event,
    unnest(native_event.displayed_offers) as offer_id
with
offset as position
where
    native_event.event_name = "ModuleDisplayedOnHomePage"
    {% if is_incremental() or target.profile_name == "CI" %}
        and date(native_event.event_date)
        between date_sub("{{ ds() }}", interval 3 day) and date("{{ ds() }}")
    {% else %} and date(native_event.event_date) >= "2024-06-13"
    {% endif %}
