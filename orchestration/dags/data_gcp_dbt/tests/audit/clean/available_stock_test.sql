-- depends_on: {{ ref('available_stock') }}
{{ 
    compare_relations(
        table_name='available_stock',
        legacy_schema_name='clean',
        primary_key='stock_id',
        legacy_table_name='available_stock_information'
    )
}}