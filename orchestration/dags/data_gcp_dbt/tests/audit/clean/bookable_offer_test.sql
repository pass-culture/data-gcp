-- depends_on: {{ ref('bookable_offer') }}
{{ 
    compare_relations(
        'bookable_offer',
        'clean',
        'offer_id',
        exclude_columns=['offer_fields_updated']
    )
}}