{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
        )
    )
}}

-- one row per tag, template, offerer and event day, aggregated over recipients
select
    brevo_tag,
    brevo_template_id,
    offerer_id,
    event_date,
    coalesce(sum(total_delivered) > 0, false) as email_is_delivered,
    coalesce(sum(total_opened) > 0, false) as email_is_opened,
    coalesce(sum(total_unsubscribed) > 0, false) as user_has_unsubscribed,
    count(
        distinct case when total_delivered > 0 then email_id end
    ) as total_recipients_delivered,
    count(
        distinct case when total_opened > 0 then email_id end
    ) as total_recipients_opened,
    count(
        distinct case when total_unsubscribed > 0 then email_id end
    ) as total_recipients_unsubscribed
from {{ ref("int_brevo__transactional_pro") }}
where
    1 = 1
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 2 day) and date("{{ ds() }}")
    {% endif %}
group by brevo_tag, brevo_template_id, offerer_id, event_date
