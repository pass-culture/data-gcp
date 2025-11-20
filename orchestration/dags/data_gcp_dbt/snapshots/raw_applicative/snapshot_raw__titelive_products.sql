{% snapshot snapshot_raw__titelive_products %}

    {{
        config(
            **custom_snapshot_config(
                strategy="check",
                check_cols=["json_hash"],
                unique_key="ean",
                hard_deletes="ignore",
                tags=["external_dependency"],
                meta={
                        "external_dag_id": "import_titelive",
                        "external_task_id": "gce_stop_task"
                }
            )
        )
    }}
    with base_data as (
        select
            ean,
            recto_image_uuid,
            verso_image_uuid,
            case
                when json_raw is null then null
                else TO_JSON_STRING(json_raw)
            end as json_str
        from {{ source("raw", "raw_titelive_products") }}
        where true
            and status = 'processed'
    )

select
        ean,
        json_str,
        recto_image_uuid,
        verso_image_uuid,
        case
            when json_str is null then null
            else TO_HEX(MD5(json_str))
        end as json_hash
    from base_data
{% endsnapshot %}
