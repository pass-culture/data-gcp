-- depends_on: {{ ref('collective_offer_domain_name') }}
{{ 
    compare_relations(
        'collective_offer_domain_name',
        'clean',
        'collective_offer_id'
    )
}}