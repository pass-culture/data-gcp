{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
            require_partition_filter=true,
        )
    )
}}


select
    native_event.event_date,
    offer_id_split as offer_id,
    module_id,
    entry_id,
    user_id,
    unique_session_id,
    position + 1 as displayed_position
from
    {{ ref("int_firebase__native_event") }} as native_event,
    unnest(displayed_offers) as offer_id_split
with
offset as position
where
    native_event.event_name = "ModuleDisplayedOnHomePage"
    {% if is_incremental() %}
        and date(event_date) = date_sub('{{ ds() }}', interval 3 day)
    {% else %} and date(event_date) >= "2024-06-13"
    {% endif %}
