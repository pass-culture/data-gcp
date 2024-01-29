-- depends_on: {{ ref('cleaned_stock') }}
{{ 
    compare_relations(
        'cleaned_stock',
        'clean',
        'stock_id'
    )
}}