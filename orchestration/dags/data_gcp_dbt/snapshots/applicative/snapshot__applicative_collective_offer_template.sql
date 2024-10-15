{% snapshot snapshot__applicative_collective_offer_template %}

    {{
        config(
            strategy="timestamp",
            unique_key="collective_offer_id",
            updated_at="collective_offer_date_updated",
        )
    }}

    with
        add_domains as (
            select
                collective_offer_template_id as collective_offer_id,
                string_agg(educational_domain_name) as educational_domains
            from
                {{
                    source(
                        "raw", "applicative_database_collective_offer_template_domain"
                    )
                }}
            left join
                {{ source("raw", "applicative_database_educational_domain") }} using (
                    educational_domain_id
                )
            group by 1
        ),

        formated_offer_template as (
            select
                template.collective_offer_last_validation_date,
                template.collective_offer_validation,
                template.collective_offer_id,
                template.collective_offer_is_active,
                template.venue_id,
                template.collective_offer_name,
                template.collective_offer_creation_date,
                template.collective_offer_subcategory_id,
                cast(
                    template.collective_offer_date_updated as timestamp
                ) as collective_offer_date_updated,
                template.collective_offer_students,
                template.collective_offer_booking_email,
                template.collective_offer_offer_venue,
                template.collective_offer_last_validation_type,
                add_domains.educational_domains,
                template.collective_offer_venue_address_type
            from
                {{ source("raw", "applicative_database_collective_offer_template") }}
                as template
            left join
                add_domains
                on template.collective_offer_id = add_domains.collective_offer_id
        )

    select *
    from formated_offer_template

{% endsnapshot %}
