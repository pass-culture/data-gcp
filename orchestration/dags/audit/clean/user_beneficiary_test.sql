-- depends_on: {{ ref('user_beneficiary') }}
{{ 
    compare_relations(
        'user_beneficiary',
        'clean',
        'user_id'
    )
}}