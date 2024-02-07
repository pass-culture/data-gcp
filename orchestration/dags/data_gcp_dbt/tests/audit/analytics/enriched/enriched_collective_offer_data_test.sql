-- depends_on: {{ ref('enriched_collective_offer_data') }}
{{ 
    compare_relations(
        'enriched_collective_offer_data',
        'analytics',
        'collective_offer_id'
    )
}}