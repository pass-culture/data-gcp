{{ config(materialized="table") }}

{% set product_types = {"book": "'paper'", "music": "'music'"} %}
{% set path_code_support = "'$.article[0].codesupport'" %}
{% set regex_paper_support = "r'[a-zA-Z]'" %}

with
    last_successful_sync as (
        select
            max(
                case when payload = {{ product_types.book }} then date end
            ) as paper_last_sync_date,
            max(
                case when payload = {{ product_types.music }} then date end
            ) as music_last_sync_date
        from {{ source("raw", "applicative_database_local_provider_event") }}
        where
            providerid in ('9', '16', '17', '19', '20', '1082', '2156', '2190')
            and payload in ({{ product_types.book }}, {{ product_types.music }})
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
                when
                    regexp_contains(
                        json_value(snap.json_raw, {{ path_code_support }}),
                        {{ regex_paper_support }}
                    )
                then {{ product_types.book }}
                else {{ product_types.music }}
            end as product_type
        from snap
        cross join last_successful_sync
        where
            (
                regexp_contains(
                    json_value(snap.json_raw, {{ path_code_support }}),
                    {{ regex_paper_support }}
                )
                and snap.dbt_valid_from
                > timestamp(last_successful_sync.paper_last_sync_date)
            )
            or (
                not regexp_contains(
                    json_value(snap.json_raw, {{ path_code_support }}),
                    {{ regex_paper_support }}
                )
                and snap.dbt_valid_from
                > timestamp(last_successful_sync.music_last_sync_date)
            )
    )

select
    delta.product_type,
    delta.ean,
    delta.recto_image_uuid as recto_uuid,
    delta.verso_image_uuid as verso_uuid,
    delta.dbt_valid_from as modification_date,

    -- Common fields
    json_value(delta.json_raw, '$.titre') as name,  -- noqa: RF04
    json_value(delta.json_raw, '$.article[0].resume') as description,
    json_value(delta.json_raw, {{ path_code_support }}) as support_code,
    json_value(delta.json_raw, '$.article[0].dateparution') as publication_date,
    json_value(delta.json_raw, '$.article[0].editeur') as publisher,
    json_query(delta.json_raw, '$.article[0].gtl') as gtl,
    cast(json_value(delta.json_raw, '$.article[0].prix') as numeric) as price,
    cast(json_value(delta.json_raw, '$.article[0].image') as int64) as image,
    cast(json_value(delta.json_raw, '$.article[0].image_4') as int64) as image_4,

    -- fields specific to PAPER
    cast(
        json_value(delta.json_raw, '$.article[0].id_lectorat') as int64
    ) as readership_id,
    json_value(delta.json_raw, '$.article[0].langueiso') as language_iso,
    json_value(delta.json_raw, '$.article[0].taux_tva') as vat_rate,
    json_query(delta.json_raw, '$.auteurs_multi') as multiple_authors,

    -- fields specific to MUSIC
    json_value(delta.json_raw, '$.article[0].artiste') as artist,
    json_value(delta.json_raw, '$.article[0].label') as music_label,
    json_value(delta.json_raw, '$.article[0].compositeur') as composer,
    json_value(delta.json_raw, '$.article[0].interprete') as performer,
    json_value(delta.json_raw, '$.article[0].nb_galettes') as nb_discs

from changed_products_snapshot as delta
