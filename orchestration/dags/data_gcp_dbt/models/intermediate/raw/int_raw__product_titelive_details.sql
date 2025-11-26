{{ config(materialized="table") }}
with
    base_data as (
        select ean, parse_json(json_value(parse_json(json_str), '$')) as json_raw
        from {{ ref("snapshot_raw__titelive_products") }}
        where dbt_valid_to is null and json_str is not null
    )

select
    base_data.ean,
    json_value(base_data.json_raw, '$.titre') as title,
    json_query(base_data.json_raw, '$.auteurs_multi') as authors,
    json_query(base_data.json_raw, '$.article[0].contributor') as contributor,
    json_value(base_data.json_raw, '$.article[0].langueiso') as language_iso,
    nullif(
        json_value(base_data.json_raw, '$.article[0].serie'), 'Non précisée'
    ) as series,
    nullif(json_value(base_data.json_raw, '$.article[0].idserie'), '0') as series_id
from base_data
