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

    filtered_products as (
        select current_meta.*
        from {{ ref("int_raw__product_titelive_details") }} as current_meta
        cross join last_successful_sync
        where
            current_meta.product_type is not null
            and (
                {% for key, val in cfg.products.items() %}
                    (
                        current_meta.product_type = '{{ val.payload_type }}'
                        and current_meta.last_updated_at
                        > timestamp(last_successful_sync.{{ key }}_last_sync_date)
                    )
                    {% if not loop.last %} or {% endif %}
                {% endfor %}
            )
    )

select
    filtered.*, snap.recto_image_uuid as recto_uuid, snap.verso_image_uuid as verso_uuid
from filtered_products as filtered
left join
    {{ ref("snapshot_raw__titelive_products") }} as snap
    on filtered.ean = snap.ean
    and snap.dbt_valid_to is null
