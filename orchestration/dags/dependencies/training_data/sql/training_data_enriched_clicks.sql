select distinct
    clicks.event_date,
    clicks.user_id,
    clicks.item_id,
    clicks.event_hour,
    clicks.event_day,
    clicks.event_month,
    clicks.timestamp,
    clicks.previous_item_id,
    * except (
        event_date, user_id, item_id, event_hour, event_day, event_month, timestamp,previous_item_id
    )
from
    (
        select
            event_date, user_id, item_id, event_hour, event_day, event_month, timestamp,previous_item_id
        from `{{ bigquery_raw_dataset }}`.training_data_clicks
    ) as clicks
left join
    `{{ bigquery_raw_dataset }}`.recommendation_user_features as user_features
    on user_features.user_id = clicks.user_id
left join
    `{{ bigquery_raw_dataset }}`.recommendation_item_features as item_features
    on item_features.item_id = clicks.item_id
