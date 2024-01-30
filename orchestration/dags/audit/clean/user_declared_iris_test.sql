-- depends_on: {{ ref('user_declared_iris') }}
{{ 
    compare_relations(
        'user_declared_iris',
        'clean',
        'user_id'
    )
}}