{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

select
    environement,
    user_id,
    offerer_id,
    message,
    booking_id,
    offer_id,
    venue_id,
    product_id,
    stock_id,
    stock_old_quantity,
    stock_new_quantity,
    stock_old_price,
    stock_new_price,
    stock_booking_quantity,
    list_of_eans_not_found,
    log_timestamp,
    partition_date,
    beta_test_new_nav_is_convenient,
    beta_test_new_nav_is_pleasant,
    beta_test_new_nav_comment,
    technical_message_id,
    choice_datetime,
    device_id,
    analytics_source,
    cookies_consent_mandatory,
    cookies_consent_accepted,
    cookies_consent_refused,
    user_satisfaction,
    user_comment,
    offer_data_api_call_id,
    offer_subcategory_chosen,
    offer_subcategories_suggested
from {{ ref("int_pcapi__log") }}
where
    (
        analytics_source = "app-pro"
        or message in (
            "Booking has been cancelled",
            "Offer has been created",
            "Offer has been updated",
            "Booking was marked as used",
            "Booking was marked as unused",
            "Successfully updated stock",
            "Some provided eans were not found",
            "Stock update blocked because of price limitation",
            "User with new nav activated submitting review",
            "User submitting review",
            "Offer Categorisation Data API"
        )
    )
    {% if is_incremental() %}
        and partition_date
        between date_sub(date("{{ ds() }}"), interval 2 day) and date("{{ ds() }}")
    {% endif %}
