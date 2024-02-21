-- depends_on: {{ ref('isbn_rayon_editor') }}
{{ 
    compare_relations(
        'isbn_rayon_editor',
        'analytics',
        'isbn'
    )
}}