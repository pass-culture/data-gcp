-- depends_on: {{ ref('enriched_collective_booking_data') }}
{{ 
    compare_relations(
        'enriched_collective_booking_data',
        'analytics',
        'collective_booking_id'
    )
}}