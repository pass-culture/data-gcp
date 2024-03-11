-- depends_on: {{ ref('enriched_deposit_data') }}
{{ 
    compare_relations(
        'enriched_deposit_data',
        'analytics',
        'deposit_id'
    )
}}