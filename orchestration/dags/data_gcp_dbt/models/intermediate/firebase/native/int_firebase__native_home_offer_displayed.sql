{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date'},
        on_schema_change = "sync_all_columns",
        require_partition_filter = true
    )
) }}


SELECT
  native_event.event_date,
  offer_id_split as offer_id,
  module_id,
  entry_id,
  position + 1 as displayed_position
FROM {{ ref('int_firebase__native_event') }} native_event,
  unnest(displayed_offers) as offer_id_split WITH OFFSET as position
WHERE native_event.event_name = "ModuleDisplayedOnHomePage"
{% if is_incremental() %}
    AND date(event_date) = date_sub('{{ ds() }}', INTERVAL 3 day)
    {% else %}
    AND date(event_date) >= "2024-06-13"
    {% endif %}
