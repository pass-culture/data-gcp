-- depends_on: {{ ref('enriched_booking_data') }}
{{ 
    compare_relations(
        'enriched_booking_data',
        'analytics',
        'booking_id'
    )
}}