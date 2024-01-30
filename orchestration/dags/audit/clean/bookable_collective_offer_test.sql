-- depends_on: {{ ref('bookable_collective_offer') }}
{{ 
    compare_relations(
        'bookable_collective_offer',
        'clean',
        'collective_offer_id'
    )
}}