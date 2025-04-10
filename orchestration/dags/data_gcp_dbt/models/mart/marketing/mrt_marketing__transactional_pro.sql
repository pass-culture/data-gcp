{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
        )
    )
}}

select
    brevo_tag,
    brevo_template_id,
    email_id,
    offerer_id,
    event_date,
    coalesce(total_delivered > 0, false) as email_is_delivered,
    coalesce(total_opened > 0, false) as email_is_opened,
    coalesce(total_unsubscribed > 0, false) as user_has_unsubscribed
from {{ ref("int_brevo__transactional_pro") }}
where
    1 = 1
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 2 day) and date("{{ ds() }}")
    {% endif %}
