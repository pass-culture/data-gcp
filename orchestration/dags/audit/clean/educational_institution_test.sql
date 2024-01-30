-- depends_on: {{ ref('educational_institution') }}
{{ 
    compare_relations(
        'educational_institution',
        'clean',
        'educational_institution_id'
    )
}}