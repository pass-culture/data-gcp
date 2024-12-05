select distinct
    click.event_date,
    click.user_id,
    click.item_id,
    click.event_hour,
    click.event_day,
    click.event_month,
    * except (event_date, user_id, item_id, event_hour, event_day, event_month)
from
    (
        select event_date, user_id, item_id, event_hour, event_day, event_month
        from {{ ref("ml_reco__training_data_click") }}
    ) as click
left join
    {{ ref("ml_reco__training_user_feature") }} as user_feature
    on user_feature.user_id = click.user_id
left join
    {{ ref("ml_reco__training_item_feature") }} as item_feature
    on item_feature.item_id = click.item_id
