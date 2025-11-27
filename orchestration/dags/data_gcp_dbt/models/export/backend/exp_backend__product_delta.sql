{{ config(materialized="table") }}


{% set provider_ids = ["9", "16", "17", "19", "20", "1082", "2156", "2190"] %}

{% set path_code_support = "$.article[0].codesupport" %}

{% set common_fields = [
    "json_value(delta.json_raw, '$.titre') as name",
    "json_value(delta.json_raw, '$.article[0].resume') as description",
    "json_value(delta.json_raw, '" ~ path_code_support ~ "') as support_code",
    "json_value(delta.json_raw, '$.article[0].dateparution') as publication_date",
    "json_value(delta.json_raw, '$.article[0].editeur') as publisher",
    "json_query(delta.json_raw, '$.article[0].gtl') as gtl",
    "cast(json_value(delta.json_raw, '$.article[0].prix') as numeric) as price",
    "cast(json_value(delta.json_raw, '$.article[0].image') as int64) as image",
    "cast(json_value(delta.json_raw, '$.article[0].image_4') as int64) as image_4"
] %}

{% set specific_paper_fields = {
    "readership_id": "cast(json_value(delta.json_raw, '$.article[0].id_lectorat') as int64)",
    "language_iso": "json_value(delta.json_raw, '$.article[0].langueiso')",
    "vat_rate": "json_value(delta.json_raw, '$.article[0].taux_tva')",
    "multiple_authors": "json_query(delta.json_raw, '$.auteurs_multi')"
} %}

{% set specific_music_fields = {
    "artist": "json_value(delta.json_raw, '$.article[0].artiste')",
    "music_label": "json_value(delta.json_raw, '$.article[0].label')",
    "composer": "json_value(delta.json_raw, '$.article[0].compositeur')",
    "performer": "json_value(delta.json_raw, '$.article[0].interprete')",
    "nb_discs": "json_value(delta.json_raw, '$.article[0].nb_galettes')"
} %}

{% set products = {
    "paper": {
        "payload_type": "paper",
        "case_condition": "regexp_contains(json_value(snap.json_raw, '" ~ path_code_support ~ "'), 'r[a-zA-Z]')",
        "specific_fields": specific_paper_fields
    },
    "music": {
        "payload_type": "music",
        "case_condition": "true",
        "specific_fields": specific_music_fields
    }
} %}

with
    last_successful_sync as (
        select
            {% for key, p in products.items() %}
            max(case when payload = '{{ p.payload_type }}' then date end) as {{ key }}_last_sync_date
            {% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ source("raw", "applicative_database_local_provider_event") }}
        where
            providerid in (
                {% for id in provider_ids -%}
                    '{{ id }}'{% if not loop.last %}, {% endif %}
                {% endfor -%}
            )
            and payload in (
                {% for key, p in products.items() -%}
                    '{{ p.payload_type }}'{% if not loop.last %}, {% endif %}
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
            when {{ products.paper.case_condition }} then '{{ products.paper.payload_type }}'
            else '{{ products.music.payload_type }}'
        end as product_type

        from snap
        cross join last_successful_sync
        where
            {% for key, p in products.items() if key != "music" %}
            (
                {{ p.case_condition }}
                and snap.dbt_valid_from > timestamp(last_successful_sync.{{ key }}_last_sync_date)
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
    {% for expr in common_fields %}
        {{ expr }}{% if not loop.last %},{% endif -%}
    {% endfor -%},

    -- Product-specific fields
    {% for key, p in products.items() %}
        {% for field_name, expr in p.specific_fields.items() -%}
            {{ expr }} as {{ field_name }}{% if not loop.last %},{% endif %}
        {% endfor -%}
        {% if not loop.last %},{% endif %}
    {% endfor -%}

from changed_products_snapshot as delta
