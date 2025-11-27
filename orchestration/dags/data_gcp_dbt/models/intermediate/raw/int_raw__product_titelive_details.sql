{{ config(materialized="table") }}

{% set details_fields = [
    {"json_path": "$.titre", "alias": "title"},
    {"json_path": "$.auteurs_multi", "alias": "authors"},
    {"json_path": "$.article[0].contributor", "alias": "contributor"},
    {"json_path": "$.article[0].langueiso", "alias": "language_iso"},
    {
        "json_path": "$.article[0].serie",
        "alias": "series",
        "null_when_equal": "Non précisée",
    },
    {
        "json_path": "$.article[0].idserie",
        "alias": "series_id",
        "null_when_equal": "0",
    },
] %}

with
    base_data as (
        select ean, parse_json(json_value(parse_json(json_str), '$')) as json_raw
        from {{ ref("snapshot_raw__titelive_products") }}
        where dbt_valid_to is null and json_str is not null
    )

select base_data.ean, {{ render_json_fields("base_data", "json_raw", details_fields) }}
from base_data
