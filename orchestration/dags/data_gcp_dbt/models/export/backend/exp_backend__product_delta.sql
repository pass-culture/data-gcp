{{ config(materialized="table") }}


with
    last_successful_sync as (
        select max(date) as last_sync_date
        from {{ source("raw", "applicative_database_local_provider_event") }}
        where providerid in ('1082', '2190') and payload = 'paper' and type = 'SyncEnd'
    ),

    changed_products_snapshot as (
        select
            snap.ean,
            snap.json_raw,
            snap.recto_image_uuid,
            snap.verso_image_uuid,
            snap.dbt_valid_from
        from {{ ref("snapshot_raw__titelive_products") }} as snap
        where
            snap.dbt_valid_to is null
            and snap.dbt_valid_from
            > (select timestamp(last_sync_date) from last_successful_sync)
            and snap.json_raw is not null
            and regexp_contains(
                json_value(snap.json_raw, '$.article[0].codesupport'), r'[a-zA-Z]'
            )
    ),

    existing_backend_products as (
        select distinct ean
        from {{ source("raw", "applicative_database_product") }}
        where
            ean is not null
            and lastproviderid in (9, 16, 17, 19, 20, 1082, 2190)
            and subcategoryid = 'LIVRE_PAPIER'
    )

select
    delta.ean,

    json_value(json_raw, '$.titre') as titre,
    json_value(json_raw, '$.article[0].resume') as resume,
    json_value(json_raw, '$.article[0].codesupport') as codesupport,
    json_value(json_raw, '$.article[0].dateparution') as dateparution,
    json_value(json_raw, '$.article[0].editeur') as editeur,
    json_value(json_raw, '$.article[0].langueiso') as langueiso,
    json_value(json_raw, '$.article[0].taux_tva') as taux_tva,
    cast(json_value(json_raw, '$.article[0].prix') as numeric) as prix,
    cast(json_value(json_raw, '$.article[0].id_lectorat') as int64) as id_lectorat,

    parse_json(json_query(json_raw, '$.article[0].gtl')) as gtl,
    parse_json(json_query(json_raw, '$.auteurs_multi')) as auteurs_multi,

    delta.recto_image_uuid as recto_uuid,
    delta.verso_image_uuid as verso_image_uuid,

    delta.dbt_valid_from as product_modification_date,
    case
        when backend.ean is null then 'add' when backend.ean is not null then 'update'
    end as action
from changed_products_snapshot as delta
left join existing_backend_products as backend on delta.ean = backend.ean
