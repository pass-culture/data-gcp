-- depends_on: {{ ref('enriched_user_offerer') }}
{{ 
    compare_relations(
        'enriched_user_offerer',
        'analytics',
        'user_offerer_id'
    )
}}