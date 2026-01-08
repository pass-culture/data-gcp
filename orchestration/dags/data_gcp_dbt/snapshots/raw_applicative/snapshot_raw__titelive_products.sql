{% snapshot snapshot_raw__titelive_products %}

    {{
        config(
            **custom_snapshot_config(
                strategy="check",
                check_cols=["json_raw"],
                unique_key="ean",
                cluster_by=["ean"],
                hard_deletes="ignore",
                tags=["external_dependency"],
                meta={
                    "external_dag_id": "import_titelive",
                    "external_task_id": "gce_stop_task",
                },
            )
        )
    }}
    select ean, recto_image_uuid, verso_image_uuid, json_raw
    from {{ source("raw", "raw_titelive_products") }}
    where true and status = 'processed'

{% endsnapshot %}
