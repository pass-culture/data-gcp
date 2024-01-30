-- depends_on: {{ ref('user_suspension') }}
{{ 
    compare_relations(
        'user_suspension',
        'clean',
        'action_history_id'
    )
}}