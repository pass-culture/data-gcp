-- depends_on: {{ ref('cleaned_stock') }}
{{ 
    compare_relations(
        'cleaned_stock',
        'clean',
        'stock_id',
        exclude_columns=['stock_fields_updated']
    )
}}