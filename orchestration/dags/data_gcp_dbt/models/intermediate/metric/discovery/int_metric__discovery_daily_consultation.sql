{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'}
    )
) }}

{% set entities = [
    {'entity': 'item_id', 'type': 'item'},
    {'entity': 'offer_subcategory_id', 'type': 'offer_subcategory'},
    {'entity': 'offer_category_id', 'type': 'offer_category'}
] %}

WITH raw_data AS (
SELECT c.consultation_id,
    c.consultation_timestamp,
    c.consultation_date,
    c.user_id,
    o.item_id,
    o.offer_subcategory_id,
    o.offer_category_id,
FROM {{ ref('int_firebase__native_consultation') }} AS c
LEFT JOIN {{ ref('int_applicative__offer') }} AS o
    ON c.offer_id = o.offer_id
{% if is_incremental() %}
WHERE consultation_date >= date_sub('{{ ds() }}', INTERVAL 3 day)
{% endif %}
)

{% for entity in entities %}
SELECT DISTINCT consultation_id,
    consultation_timestamp,
    consultation_date,
    user_id,
    {{ entity.entity }} AS consulted_entity,
    '{{ entity.type }}' AS type
FROM raw_data
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
