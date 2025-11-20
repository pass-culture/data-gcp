{{ config(materialized="table") }}


with
    last_successful_sync as (
        select max(date) as last_sync_date
        from {{ source("raw", "applicative_database_local_provider_event") }}
        where
            providerid in ('9', '16', '17', '19', '20', '1082', '2156', '2190')
            and payload = 'paper'
            and type = 'SyncEnd'
    ),

    snap as (
        select
            ean,
            recto_image_uuid,
            verso_image_uuid,
            dbt_valid_from,
            dbt_valid_to,
            parse_json(json_str) as json_raw
        from {{ ref("snapshot_raw__titelive_products") }}
        where dbt_valid_to is null and json_str is not null
    ),

    changed_products_snapshot as (
        select
            snap.ean,
            snap.json_raw,
            snap.recto_image_uuid,
            snap.verso_image_uuid,
            snap.dbt_valid_from
        from snap
        where
            snap.dbt_valid_from
            > (select timestamp(lss.last_sync_date) from last_successful_sync as lss)
            and regexp_contains(
                json_value(snap.json_raw, '$.article[0].codesupport'), r'[a-zA-Z]'
            )
    )

select
    delta.ean,
    cast(json_value(delta.json_raw, '$.article[0].prix') as numeric) as price,
    cast(
        json_value(delta.json_raw, '$.article[0].id_lectorat') as int64
    ) as readership_id,
    delta.recto_image_uuid as recto_uuid,
    delta.verso_image_uuid as verso_uuid,
    cast(json_value(delta.json_raw, '$.article[0].image') as int64) as image,
    cast(json_value(delta.json_raw, '$.article[0].image_4') as int64) as image_4,
    delta.dbt_valid_from as modification_date,
    json_value(delta.json_raw, '$.titre') as name,
    json_value(delta.json_raw, '$.article[0].resume') as description,
    json_value(delta.json_raw, '$.article[0].codesupport') as support_code,
    json_value(delta.json_raw, '$.article[0].dateparution') as publication_date,
    json_value(delta.json_raw, '$.article[0].editeur') as publisher,
    json_value(delta.json_raw, '$.article[0].langueiso') as language_iso,
    json_value(delta.json_raw, '$.article[0].taux_tva') as vat_rate,
    parse_json(json_query(delta.json_raw, '$.article[0].gtl')) as gtl,
    parse_json(json_query(delta.json_raw, '$.auteurs_multi')) as multiple_authors
from changed_products_snapshot as delta
