{% set cfg = titelive_product_setup() %}

{{ config(materialized="table") }}

with
    last_successful_sync as (
        select
            {% for key, val in cfg.products.items() %}
                max(
                    case when payload = '{{ val.payload_type }}' then date end
                ) as {{ key }}_last_sync_date
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ source("raw", "applicative_database_local_provider_event") }}
        where
            providerid in (
                {%- for id in cfg.provider_ids -%}
                    '{{ id }}'{%- if not loop.last %},{% endif -%}
                {%- endfor -%}
            )
            and payload in (
                {%- for key, val in cfg.products.items() -%}
                    '{{ val.payload_type }}'{%- if not loop.last %},{% endif -%}
                {% endfor -%}
            )
            and type = 'SyncEnd'
    ),

    snap as (
        select
            ean,
            recto_image_uuid,
            verso_image_uuid,
            dbt_valid_from,
            dbt_valid_to,
            parse_json(json_value(parse_json(json_str), '$')) as json_raw
        from {{ ref("snapshot_raw__titelive_products") }}
        where dbt_valid_to is null and json_str is not null
    ),

    changed_products_snapshot as (
        select
            snap.ean,
            snap.json_raw,
            snap.recto_image_uuid,
            snap.verso_image_uuid,
            snap.dbt_valid_from,

            case
                {%- for key, val in cfg.products.items() %}
                    when regexp_contains(
                        json_value(snap.json_raw, '{{ cfg.path_code_support }}'),
                        '{{ val.support_code_pattern }}'
                    ) then '{{ val.payload_type }}'
                {% endfor -%}
                else null
            end as product_type

        from snap
        cross join last_successful_sync
        where
            {% for key, val in cfg.products.items() %}
                (
                    regexp_contains(
                        json_value(snap.json_raw, '{{ cfg.path_code_support }}'),
                        '{{ val.support_code_pattern }}'
                    )
                    and snap.dbt_valid_from
                    > timestamp(last_successful_sync.{{ key }}_last_sync_date)
                )
                {% if not loop.last %} or {% endif %}
            {% endfor %}
    )

select
    delta.product_type,
    delta.ean,
    delta.recto_image_uuid as recto_uuid,
    delta.verso_image_uuid as verso_uuid,
    delta.dbt_valid_from as modification_date,
    -- Common fields
    {{- render_json_fields("delta", "json_raw", cfg.common_fields_struct) }},
    -- Product-specific fields
    {%- for key, val in cfg.products.items() -%}
        {{- render_json_fields("delta", "json_raw", val.specific_fields) }}
        {%- if not loop.last %},{% endif -%}
    {%- endfor %}

from changed_products_snapshot as delta
