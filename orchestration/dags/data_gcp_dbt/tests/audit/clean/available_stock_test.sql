-- depends_on: {{ ref('available_stock_information') }}
{{ 
    compare_relations(
        table_name='available_stock_information',
        legacy_schema_name='clean',
        primary_key='stock_id',
        exclude_columns=['available_stock_information']
    )
}}