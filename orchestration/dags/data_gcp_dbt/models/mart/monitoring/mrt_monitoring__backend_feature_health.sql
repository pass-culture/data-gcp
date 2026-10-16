{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

with
    -- features reporting failures (not plain event logs): any origin/feature/action
    -- that logged at least one failure is monitored, new ones are picked up
    -- automatically
    monitored_features as (
        select distinct origin, feature, action
        from {{ ref("int_pcapi__feature_log") }}
        where partition_date >= '2026-09-17' and not is_success
    )

select
    feature_log.partition_date,
    feature_log.origin,
    feature_log.feature,
    feature_log.action,
    feature_log.technical_message_id,
    feature_log.severity,
    feature_log.exception_class,
    feature_log.provider_id,
    count(*) as total_count,
    countif(feature_log.is_success) as success_count,
    countif(not feature_log.is_success) as error_count
from {{ ref("int_pcapi__feature_log") }} as feature_log
inner join
    monitored_features
    on feature_log.origin = monitored_features.origin
    and feature_log.feature = monitored_features.feature
    and feature_log.action = monitored_features.action
where
    {% if is_incremental() %}
        feature_log.partition_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% else %} feature_log.partition_date >= '2026-09-17'
    {% endif %}
group by
    feature_log.partition_date,
    feature_log.origin,
    feature_log.feature,
    feature_log.action,
    feature_log.technical_message_id,
    feature_log.severity,
    feature_log.exception_class,
    feature_log.provider_id
