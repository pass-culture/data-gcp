-- depends_on: {{ ref('user_reco_iris') }}
{{ 
    compare_relations(
        'user_reco_iris',
        'clean',
        'user_id' ~ 'month_log'
    )
}}