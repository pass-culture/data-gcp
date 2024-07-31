{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

select
    THUMBCOUNT as THUMB_COUNT,
    IDATPROVIDERS as ID_AT_PROVIDERS,
    DATEMODIFIEDATLASTPROVIDER as DATE_MODIFIED_AT_LAST_PROVIDER,
    ID,
    DATECREATED as DATE_CREATED,
    AUTHORID as AUTHOR_ID,
    LASTPROVIDERID as LAST_PROVIDER_ID,
    OFFERID as OFFER_ID,
    CREDIT,
    ISACTIVE as IS_ACTIVE,
    FIELDSUPDATED as FIELDS_UPDATED,
    case when ISACTIVE and THUMBCOUNT > 0 then 1 else 0 end as IS_MEDIATION,
    {{ target_schema }}.humanize_id(ID) as MEDIATION_HUMANIZED_ID,
    ROW_NUMBER() over (partition by OFFERID order by DATEMODIFIEDATLASTPROVIDER desc) as MEDIATION_ROWN
from {{ source('raw', 'applicative_database_mediation') }}
