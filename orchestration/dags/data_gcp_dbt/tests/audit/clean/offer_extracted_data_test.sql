-- depends_on: {{ ref('offer_extracted_data') }}
{{ 
    compare_relations(
        'offer_extracted_data',
        'analytics',
        'offer_id'
    )
}}