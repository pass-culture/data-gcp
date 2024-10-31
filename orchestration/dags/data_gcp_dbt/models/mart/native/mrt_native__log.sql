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
    age,
    bookings_count,
    first_deposit_activation_date,
    status,
    technical_message_id,
    feedback
from {{ ref("int_pcapi__log") }}
where
    technical_message_id = 'user_feedback'
    {% if is_incremental() %}
        and partition_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% endif %}
