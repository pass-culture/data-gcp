{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "booking_creation_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set entities = [
    {
        "entity": "offer_category_id",
        "type": "OFFER_CATEGORY",
        "score_multiplier": 25,
    },
    {
        "entity": "venue_type_label",
        "type": "VENUE_TYPE",
        "score_multiplier": 20,
    },
    {
        "entity": "offer_subcategory_id",
        "type": "OFFER_SUBCATEGORY",
        "score_multiplier": 10,
    },
    {"entity": "venue_id", "type": "VENUE", "score_multiplier": 5},
    {
        "entity": "extra_category",
        "type": "EXTRA_CATEGORY",
        "score_multiplier": 5,
    },
] %}

with
    raw_data as (
        select
            b.booking_id,
            b.booking_created_at,
            b.booking_creation_date,
            b.user_id,
            b.deposit_id,
            s.offer_subcategory_id,
            s.venue_type_label,
            s.offer_category_id,
            s.venue_id,
            coalesce(om.offer_type_label, s.venue_id) as extra_category,  -- TODO: venue_id is used as extra_category when offer_type_label is null
            row_number() over (
                partition by b.user_id order by b.booking_created_at
            ) as booking_rank
        from {{ ref("int_applicative__booking") }} as b
        left join {{ ref("int_global__stock") }} as s on b.stock_id = s.stock_id
        left join
            {{ ref("int_applicative__offer_metadata") }} as om
            on s.offer_id = om.offer_id
        where b.booking_status != 'CANCELLED'
    ),

    entity_calculations as (
        {% for entity in entities %}
            select distinct
                booking_id,
                booking_created_at,
                booking_creation_date,
                booking_rank,
                user_id,
                deposit_id,
                {{ entity.entity }} as diversity_booked_entity,
                '{{ entity.type }}' as diversity_booked_entity_type,
                {{ entity.score_multiplier }} as score_multiplier,
                row_number() over (
                    partition by user_id, {{ entity.entity }}
                    order by booking_created_at
                ) as diversity_booking_entity_rank
            from raw_data
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    )

select
    booking_rank,
    booking_id,
    booking_created_at,
    diversity_booking_entity_rank,
    diversity_booked_entity_type,
    diversity_booked_entity,
    user_id,
    deposit_id,
    booking_creation_date,
    case
        when diversity_booking_entity_rank = 1 then score_multiplier else 0
    end as diversity_score
from entity_calculations
