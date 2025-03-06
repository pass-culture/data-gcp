{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "user_feedback_creation_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
        )
    )
}}

select
    partition_date as user_feedback_creation_date,
    user_id,
    user_age,
    total_bookings,
    user_first_deposit_activation_date,
    user_status,
    user_feedback_message
from {{ ref("int_pcapi__log") }}
where
    technical_message_id = 'user_feedback'
    {% if is_incremental() %}
        and partition_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% else %} and partition_date >= "2024-08-01"  -- feature deployment date
    {% endif %}
