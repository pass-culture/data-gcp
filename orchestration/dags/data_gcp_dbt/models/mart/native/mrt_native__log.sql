{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "feedback_creation_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="sync_all_columns",
        )
    )
}}

select
    partition_date as feedback_creation_date,
    user_id,
    age as user_age,
    bookings_count as total_bookings,
    first_deposit_activation_date as user_first_deposit_activation_date,
    status as user_status,
    feedback as user_feedback_message
from {{ ref("int_pcapi__log") }}
where
    technical_message_id = 'user_feedback'
    {% if is_incremental() %}
        and partition_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% else %} and partition_date >= "2024-08-01"  -- feature deployment date
    {% endif %}
