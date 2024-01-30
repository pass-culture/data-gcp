-- depends_on: {{ ref('enriched_offer_data') }}
{{ 
    compare_relations(
        'enriched_offer_data',
        'analytics',
        'offer_id'
    )
}}