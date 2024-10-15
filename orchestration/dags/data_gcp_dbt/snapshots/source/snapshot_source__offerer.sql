{% snapshot snapshot_source__offerer %}

    {{
        config(
            **custom_snapshot_config(
                strategy="check",
                unique_key="offerer_id",
                check_cols=[
                    "offerer_name",
                    "offerer_address",
                    "offerer_postal_code",
                    "offerer_is_active",
                    "offerer_validation_status",
                    "offerer_siren",
                ],
            )
        )
    }}

    select *
    from {{ ref("raw_backend__offerer") }}

{% endsnapshot %}
