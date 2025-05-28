{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(pre_hook="{{create_humanize_id_function()}}") }}

select
    thumbcount as thumb_count,
    datemodifiedatlastprovider as date_modified_at_last_provider,
    id,
    datecreated as date_created,
    authorid as author_id,
    lastproviderid as last_provider_id,
    offerid as offer_id,
    credit,
    isactive as is_active,
    case when isactive and thumbcount > 0 then 1 else 0 end as is_mediation,
    {{ target_schema }}.humanize_id(id) as mediation_humanized_id,
    row_number() over (
        partition by offerid order by datemodifiedatlastprovider desc
    ) as mediation_rown
from {{ source("raw", "applicative_database_mediation") }}
