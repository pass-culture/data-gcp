-- depends_on: {{ ref('bookable_offer') }}
{{ 
    compare_relations(
        'bookable_offer',
        'clean',
        'offer_id'
    )
}}