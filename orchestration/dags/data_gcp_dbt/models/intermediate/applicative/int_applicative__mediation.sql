{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

SELECT
    thumbCount AS thumb_count,
    idAtProviders AS id_at_providers,
    dateModifiedAtLastProvider AS date_modified_at_last_provider,
    id,
    dateCreated AS date_created,
    authorId AS author_id,
    lastProviderId AS last_provider_id,
    offerId AS offer_id,
    credit,
    isActive AS is_active,
    fieldsUpdated AS fields_updated,
    CASE WHEN isActive AND thumbCount > 0 THEN 1 ELSE 0 END AS is_mediation,
    {{target_schema}}.humanize_id(id) AS mediation_humanized_id,
    ROW_NUMBER() OVER (PARTITION BY offerId ORDER BY dateModifiedAtLastProvider DESC) as mediation_rown
FROM {{ source('raw', 'applicative_database_mediation') }}
