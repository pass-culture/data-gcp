{{ config(materialized="table") }}

{% set provider_ids = ["9", "16", "17", "19", "20", "1082", "2156", "2190"] %}
{% set path_code_support = "$.article[0].codesupport" %}

{% set common_fields_struct = [
    {"json_path": "$.titre", "alias": "title"},
    {"json_path": "$.article[0].resume", "alias": "description"},
    {"json_path": path_code_support, "alias": "support_code"},
    {"json_path": "$.article[0].dateparution", "alias": "publication_date"},
    {"json_path": "$.article[0].editeur", "alias": "publisher"},
    {"json_path": "$.article[0].gtl", "alias": "gtl"},
    {"json_path": "$.article[0].prix", "alias": "price", "cast_type": "numeric"},
    {"json_path": "$.article[0].image", "alias": "image", "cast_type": "int64"},
    {"json_path": "$.article[0].image_4", "alias": "image_4", "cast_type": "int64"},
] %}

{% set specific_paper_fields_struct = [
    {"json_path": "$.article[0].id_lectorat", "alias": "readership_id", "cast_type": "int64"},
    {"json_path": "$.article[0].langueiso", "alias": "language_iso"},
    {"json_path": "$.article[0].taux_tva", "alias": "vat_rate"},
    {"json_path": "$.auteurs_multi", "alias": "multiple_authors"},
] %}

{% set specific_music_fields_struct = [
    {"json_path": "$.article[0].artiste", "alias": "artist"},
    {"json_path": "$.article[0].label", "alias": "music_label"},
    {"json_path": "$.article[0].compositeur", "alias": "composer"},
    {"json_path": "$.article[0].interprete", "alias": "performer"},
    {"json_path": "$.article[0].nb_galettes", "alias": "nb_discs"},
] %}

{% set products = {
    "paper": {
        "payload_type": "paper",
        "case_condition": "regexp_contains(json_value(snap.json_raw, '"
        ~ path_code_support
        ~ "'), 'r[a-zA-Z]')",
        "specific_fields": specific_paper_fields_struct,
    },
    "music": {
        "payload_type": "music",
        "case_condition": "regexp_contains(json_value(snap.json_raw, '"
        ~ path_code_support
        ~ "'), 'r[0-9]')",
        "specific_fields": specific_music_fields_struct,
    },
} %}

with
    last_successful_sync as (
        select
            {% for key, p in products.items() %}
                max(
                    case when payload = '{{ p.payload_type }}' then date end
                ) as {{ key }}_last_sync_date
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ source("raw", "applicative_database_local_provider_event") }}
        where
            providerid in (
                {%- for id in provider_ids -%}
                    '{{ id }}'{%- if not loop.last %},{% endif -%}
                {%- endfor -%}
            )
            and payload in (
                {%- for key, p in products.items() -%}
                    '{{ p.payload_type }}'{%- if not loop.last %},{% endif -%}
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
                {%- for key, p in products.items() %}
                    when {{ p.case_condition }} then '{{ p.payload_type }}'
                {% endfor -%}
                else null
            end as product_type

        from snap
        cross join last_successful_sync
        where
            {% for key, p in products.items() %}
                (
                    {{ p.case_condition }}
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
    {{- render_json_fields("delta", "json_raw", common_fields_struct) }},
    -- Product-specific fields
    {%- for key, p in products.items() -%}
        {{- render_json_fields("delta", "json_raw", p.specific_fields) }}
        {%- if not loop.last %},{% endif -%}
    {%- endfor %}

from changed_products_snapshot as delta
