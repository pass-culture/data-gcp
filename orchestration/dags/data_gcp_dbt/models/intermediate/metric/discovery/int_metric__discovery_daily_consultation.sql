{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "consultation_date", "data_type": "date"},
        )
    )
}}

{% set entities = [
    {"entity": "item_id", "type": "item"},
    {"entity": "offer_subcategory_id", "type": "offer_subcategory"},
    {"entity": "offer_category_id", "type": "offer_category"},
] %}

with
    raw_data as (
        select
            c.consultation_id,
            c.consultation_timestamp,
            c.consultation_date,
            c.user_id,
            o.item_id,
            o.offer_subcategory_id,
            o.offer_category_id,
        from {{ ref("int_firebase__native_consultation") }} as c
        left join {{ ref("int_applicative__offer") }} as o on c.offer_id = o.offer_id
        {% if is_incremental() %}
            where consultation_date >= date_sub('{{ ds() }}', interval 3 day)
        {% endif %}
    )

{% for entity in entities %}
    select distinct
        consultation_id,
        consultation_timestamp,
        consultation_date,
        user_id,
        {{ entity.entity }} as consulted_entity,
        '{{ entity.type }}' as type
    from raw_data
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
