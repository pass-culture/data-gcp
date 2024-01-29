-- depends_on: {{ ref('offer') }}
{{ 
    compare_relations(
        'offer',
        'clean',
        'offer_id'
    )
}}