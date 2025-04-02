-- depends_on: {{ ref('raw_applicative__offer_full') }}, {{ ref('raw_applicative__offer_lite') }}
{% snapshot snapshot_raw__offer %}

    {% if is_first_day_of_month() == "TRUE" %}

        {{
            config(
                **custom_snapshot_config(
                    unique_key="offer_id",
                    strategy="check",
                    check_cols=["custom_scd_id"],
                )
            )
        }}

        select
            offer_id_at_providers,
            offer_modified_at_last_provider_date,
            offer_id,
            offer_creation_date,
            offer_product_id,
            venue_id,
            offer_last_provider_id,
            booking_email,
            offer_is_active,
            offer_name,
            offer_description,
            offer_url,
            offer_is_national,
            offer_extra_data,
            offer_ean,
            offer_is_duo,
            offer_fields_updated,
            offer_withdrawal_details,
            offer_audio_disability_compliant,
            offer_mental_disability_compliant,
            offer_motor_disability_compliant,
            offer_visual_disability_compliant,
            offer_external_ticket_office_url,
            offer_validation,
            offer_last_validation_type,
            offer_subcategoryid,
            offer_updated_date,
            offer_withdrawal_type,
            offer_withdrawal_delay,
            booking_contact,
            offerer_address_id,
            custom_scd_id
        from {{ ref("raw_applicative__offer_full") }}

    {% else %}

        {{
            config(
                **custom_snapshot_config(
                    strategy="timestamp",
                    unique_key="offer_id",
                    updated_at="offer_updated_date",
                    invalidate_hard_deletes=False,
                )
            )
        }}

        select
            offer_id_at_providers,
            offer_modified_at_last_provider_date,
            offer_id,
            offer_creation_date,
            offer_product_id,
            venue_id,
            offer_last_provider_id,
            booking_email,
            offer_is_active,
            offer_name,
            offer_description,
            offer_url,
            offer_is_national,
            offer_extra_data,
            offer_ean,
            offer_is_duo,
            offer_fields_updated,
            offer_withdrawal_details,
            offer_audio_disability_compliant,
            offer_mental_disability_compliant,
            offer_motor_disability_compliant,
            offer_visual_disability_compliant,
            offer_external_ticket_office_url,
            offer_validation,
            offer_last_validation_type,
            offer_subcategoryid,
            offer_updated_date,
            offer_withdrawal_type,
            offer_withdrawal_delay,
            booking_contact,
            offerer_address_id,
            custom_scd_id
        from {{ ref("raw_applicative__offer_lite") }}

    {% endif %}

{% endsnapshot %}
