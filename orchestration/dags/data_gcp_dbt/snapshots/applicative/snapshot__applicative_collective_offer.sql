{% snapshot snapshot__applicative_collective_offer %}

    {{
        config(
            unique_key="collective_offer_id",
            strategy="timestamp",
            updated_at="collective_offer_date_updated",
        )
    }}
    with
        formated_collective_offer as (
            select
                collective_offer_last_validation_date,
                collective_offer_validation,
                collective_offer_id,
                collective_offer_is_active,
                venue_id,
                collective_offer_name,
                collective_offer_booking_email,
                collective_offer_creation_date,
                collective_offer_subcategory_id,
                cast(
                    collective_offer_date_updated as timestamp
                ) as collective_offer_date_updated,
                collective_offer_students,
                collective_offer_offer_venue,
                collective_offer_last_validation_type
            from {{ source("raw", "applicative_database_collective_offer") }}
        )

    select *
    from formated_collective_offer

{% endsnapshot %}
