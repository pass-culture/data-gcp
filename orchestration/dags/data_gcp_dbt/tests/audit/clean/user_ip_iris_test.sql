-- depends_on: {{ ref('user_ip_iris') }}
{{ 
    compare_relations(
        'user_ip_iris',
        'clean',
        primary_key=none
    )
}}