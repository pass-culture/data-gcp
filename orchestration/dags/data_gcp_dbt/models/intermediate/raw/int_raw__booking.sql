{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "dbt_valid_to", "data_type": "timestamp"},
        )
    )
}}

select
    booking_id,
    booking_creation_date,
    stock_id,
    booking_quantity,
    user_id,
    booking_amount,
    booking_status,
    booking_is_cancelled,
    booking_is_used,
    reimbursed,
    booking_used_date,
    booking_cancellation_date,
    booking_cancellation_reason,
    deposit_id,
    offerer_id,
    venue_id,
    price_category_label,
    booking_reimbursement_date,
    booking_used_recredit_type
from {{ ref("snapshot_raw__booking") }}
where {{ var("snapshot_filter") }}
