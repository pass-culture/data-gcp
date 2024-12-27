{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "booking_creation_date", "data_type": "date"},
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
            booking_id,
            booking_created_at,
            booking_creation_date,
            user_id,
            offer_subcategory_id,
            venue_type_label,
            offer_category_id,
            venue_id,
            coalesce(offer_type_label, venue_id) as extra_category,  -- venue_id is used as extra_category when offer_type_label is null
            row_number() over (
                partition by user_id order by booking_created_at
            ) as booking_rank
        from {{ ref("int_global__booking") }}
        where booking_status != 'CANCELLED'
    ),

    entity_calculations as (
        {% for entity in entities %}
            select distinct
                booking_id,
                booking_created_at,
                booking_creation_date,
                booking_rank,
                user_id,
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
    booking_creation_date,
    case
        when booking_rank = 1
        then score_multiplier
        when diversity_booking_entity_rank = 1
        then score_multiplier
        else 0
    end as diversity_score
from entity_calculations
