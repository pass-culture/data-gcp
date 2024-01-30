-- depends_on: {{ ref('enriched_offerer_data') }}
{{ 
    compare_relations(
        'enriched_offerer_data',
        'analytics',
        'offerer_id'
    )
}}