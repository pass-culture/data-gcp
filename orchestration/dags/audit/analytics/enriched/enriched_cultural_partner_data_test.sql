-- depends_on: {{ ref('enriched_cultural_partner_data') }}
{{ 
    compare_relations(
        'enriched_cultural_partner_data',
        'analytics',
        'partner_id'
    )
}}