{% snapshot snapshot__applicative_offerer_tag_mapping %}

    {{
        config(
            strategy="check",
            unique_key="offerer_tag_mapping_id",
            check_cols=["offerer_id", "tag_id"],
        )
    }}

    select offerer_tag_mapping_id, offerer_id, tag_id
    from {{ source("raw", "applicative_database_offerer_tag_mapping") }}

{% endsnapshot %}
