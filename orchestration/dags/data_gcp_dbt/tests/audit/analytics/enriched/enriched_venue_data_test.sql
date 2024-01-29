-- depends_on: {{ ref('enriched_venue_data') }}
{{ 
    compare_relations(
        'enriched_venue_data',
        'analytics',
        'venue_id'
    )
}}