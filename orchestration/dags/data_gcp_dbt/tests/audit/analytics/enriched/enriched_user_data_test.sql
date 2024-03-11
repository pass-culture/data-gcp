-- depends_on: {{ ref('enriched_user_data') }}
{{ 
    compare_relations(
        'enriched_user_data',
        'analytics',
        'user_id'
    )
}}