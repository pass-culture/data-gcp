-- depends_on: {{ ref('offer_item_ids') }}
{{ 
    compare_relations(
        'offer_item_ids',
        'analytics',
        'offer_id'
    )
}}