{% snapshot snapshot__applicative_offerer_tag_category_mapping %}

    {{
        config(
            strategy="check",
            unique_key="offerer_tag_category_mapping_id",
            check_cols=["offerer_tag_id", "offerer_tag_category_id"],
        )
    }}

    select offerer_tag_category_mapping_id, offerer_tag_id, offerer_tag_category_id
    from {{ source("raw", "applicative_database_offerer_tag_category_mapping") }}

{% endsnapshot %}
