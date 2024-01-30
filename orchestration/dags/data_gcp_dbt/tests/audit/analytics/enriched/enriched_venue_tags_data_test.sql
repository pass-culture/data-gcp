-- depends_on: {{ ref('enriched_venue_tags_data') }}
{{ 
    compare_relations(
        'enriched_venue_tags_data',
        'analytics',
        'venue_id'
    )
}}