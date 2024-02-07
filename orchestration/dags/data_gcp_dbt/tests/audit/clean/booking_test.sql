-- depends_on: {{ ref('booking') }}
{{ 
    compare_relations(
        'booking',
        'clean',
        'booking_id'
    )
}}