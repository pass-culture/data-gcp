{% snapshot snapshot_raw__titelive_products %}

    {{
        config(
            **custom_snapshot_config(
                strategy="check",
                check_cols=["json_raw"],
                unique_key="ean",
                hard_deletes="ignore",
                target_schema="tmp_cdarnis_dev"
            )
        )
    }}

    select
        ean,
        json_raw,
        recto_image_uuid,
        verso_image_uuid
    from {{ source("raw", "raw_titelive_products") }}
    where true
        and status = 'processed'

{% endsnapshot %}
