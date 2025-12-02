{% set cfg = titelive_product_setup() %}

{{
    config(
        **custom_incremental_config(
            incremental_strategy="merge",
            partition_by=None,
            cluster_by=["ean", "product_type"],
            unique_key="ean",
            on_schema_change="sync_all_columns",
        )
    )
}}

with
    base_data as (
        select
            ean,
            dbt_valid_from,
            parse_json(json_value(parse_json(json_str), '$')) as json_raw
        from {{ ref("snapshot_raw__titelive_products") }}
        where
            dbt_valid_to is null and json_str is not null
            {% if is_incremental() %}
                and dbt_valid_from > (
                    select max(this.last_updated_at) as dummy_alias
                    from {{ this }} as this
                )
            {% endif %}
    ),

    with_product_type as (
        select
            ean,
            json_raw,
            dbt_valid_from,
            case
                {% for key, val in cfg.products.items() %}
                    when
                        regexp_contains(
                            json_value(json_raw, '{{ cfg.path_code_support }}'),
                            {{ val.support_code_pattern }}
                        )
                    then '{{ val.payload_type }}'
                {% endfor %}
                else null
            end as product_type
        from base_data
    )

select
    ean,  -- paper or music
    product_type,
    dbt_valid_from as last_updated_at,
    -- Common fields
    {{ render_json_fields("with_product_type", "json_raw", cfg.common_fields_struct) }},
    -- Paper-specific fields
    {{
        render_json_fields(
            "with_product_type", "json_raw", cfg.specific_paper_fields_struct
        )
    }},
    -- Music-specific fields
    {{
        render_json_fields(
            "with_product_type", "json_raw", cfg.specific_music_fields_struct
        )
    }}
from with_product_type
