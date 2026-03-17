{% snapshot snapshot_raw__allocine_movie %}

    {{
        config(
            **custom_snapshot_config(
                strategy="timestamp",
                updated_at="updated_at",
                unique_key="movie_id",
                cluster_by=["movie_id"],
                hard_deletes="ignore",
                tags=["external_dependency"],
                meta={
                    "external_dag_id": "import_allocine",
                    "external_task_id": "gce_stop_task",
                },
                target_schema=generate_schema_name("raw_etl_" ~ target.name)
            )
        )
    }}
    select 
        movie_id,
        internalId,
        title,
        originalTitle,
        type,
        runtime,
        synopsis,
        poster_url,
        backlink_url,
        backlink_label,
        data_eidr,
        data_productionYear,
        cast_normalized,
        credits_normalized,
        releases,
        countries,
        genres,
        companies,
        content_hash,
        poster_gcs_path,
        updated_at
    from {{ source("raw", "allocine_movie") }}

{% endsnapshot %}
