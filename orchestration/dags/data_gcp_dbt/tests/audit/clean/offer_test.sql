-- depends_on: {{ ref('offer') }}
{{ 
    compare_relations(
        'offer',
        'clean',
        'offer_id',
        exclude_columns=['offer_fields_updated']
    )
}}