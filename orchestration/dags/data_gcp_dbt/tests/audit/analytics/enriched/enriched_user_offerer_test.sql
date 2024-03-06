-- depends_on: {{ ref('enriched_user_offerer_data') }}
{{ 
    compare_relations(
        'enriched_user_offerer_data',
        'analytics',
        'user_offerer_id'
    )
}}