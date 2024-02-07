-- depends_on: {{ ref('venue') }}
{{ 
    compare_relations(
        'venue',
        'clean',
        'venue_id'
    )
}}