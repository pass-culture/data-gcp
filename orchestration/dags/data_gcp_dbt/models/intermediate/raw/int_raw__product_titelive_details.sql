{{ config(materialized="table") }}

select
    snap.ean,
    json_value(snap.json_raw, '$.titre') as title,
    parse_json(json_query(snap.json_raw, '$.auteurs_multi')) as authors,
    parse_json(json_query(snap.json_raw, '$.article[0].contributor')) as contributor,
    json_value(snap.json_raw, '$.article[0].langueiso') as language_iso,
    nullif(json_value(snap.json_raw, '$.article[0].serie'), 'Non précisée') as series,
    nullif(json_value(snap.json_raw, '$.article[0].idserie'), '0') as series_id
from {{ ref("snapshot_raw__titelive_products") }} as snap
where snap.dbt_valid_to is null and snap.json_raw is not null
