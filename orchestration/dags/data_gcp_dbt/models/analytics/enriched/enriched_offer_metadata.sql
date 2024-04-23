-- TODO: 2024-04-16 legacy for metabase, will be removed later
{{
    config(
        materialized = "view"
    )
}}

SELECT * FROM  {{ ref('offer_metadata') }}