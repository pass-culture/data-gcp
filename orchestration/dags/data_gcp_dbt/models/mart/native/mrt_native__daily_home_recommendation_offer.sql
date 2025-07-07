{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "module_displayed_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

with
    module_context as (
        select distinct
            user_id,
            module_displayed_date,
            unique_session_id,
            user_location_type,
            entry_id,
            entry_name,
            module_id,
            module_name,
            parent_module_id,
            parent_module_type,
            parent_entry_id,
            parent_home_type,
            module_type,
            reco_call_id
        from {{ ref("mrt_native__daily_user_home_module") }}
        {% if is_incremental() %}
            where
                module_displayed_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
        {% else %}
            where
                module_displayed_date
                between date_sub(date('{{ ds() }}'), interval 60 day) and date(
                    '{{ ds() }}'
                )
        {% endif %}
    ),

    offer_context as (
        select
            doe.event_date as module_displayed_date,
            mc.user_id,
            mc.unique_session_id,
            doe.reco_call_id,
            doe.playlist_origin,
            doe.context,
            doe.offer_id,
            doe.offer_display_order,
            mc.user_location_type,
            mc.entry_id,
            mc.entry_name,
            mc.module_id,
            mc.module_name,
            mc.parent_module_id,
            mc.parent_module_type,
            mc.parent_entry_id,
            mc.parent_home_type,
            mc.module_type
        from {{ ref("int_pcreco__displayed_offer_event") }} doe
        inner join
            module_context mc
            on mc.module_displayed_date = doe.event_date
            and mc.reco_call_id = doe.reco_call_id
            and mc.user_id = doe.user_id
        where
            playlist_origin = "recommendation"
            {% if is_incremental() %}
                and event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
            {% else %}
                and event_date
                between date_sub(date('{{ ds() }}'), interval 60 day) and date(
                    '{{ ds() }}'
                )
            {% endif %}
    )

select
    oc.module_displayed_date,
    oc.reco_call_id,
    oc.playlist_origin,
    oc.context,
    oc.user_id,
    oc.offer_id,
    oc.offer_display_order,
    oc.user_location_type,
    oc.entry_id,
    oc.entry_name,
    oc.module_id,
    oc.module_name,
    oc.parent_module_id,
    oc.parent_module_type,
    oc.parent_entry_id,
    oc.parent_home_type,
    oc.module_type,
    oc.unique_session_id,
    mc.booking_id,
    max(mc.consult_offer_timestamp) as consult_offer_timestamp,
    max(mc.booking_timestamp) as booking_timestamp,
    max(mc.fav_timestamp) as fav_timestamp
from offer_context oc
left join
    {{ ref("mrt_native__daily_user_home_module") }} mc
    on oc.module_displayed_date = mc.module_displayed_date
    and oc.reco_call_id = mc.reco_call_id
    and oc.playlist_origin = mc.module_type
    and oc.offer_id = mc.offer_id
group by
    module_displayed_date,
    reco_call_id,
    playlist_origin,
    context,
    user_id,
    offer_id,
    offer_display_order,
    user_location_type,
    entry_id,
    entry_name,
    module_id,
    module_name,
    parent_module_id,
    parent_module_type,
    parent_entry_id,
    parent_home_type,
    module_type,
    unique_session_id,
    booking_id
