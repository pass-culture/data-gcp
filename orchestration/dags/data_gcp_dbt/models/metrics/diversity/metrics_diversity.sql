{% set min_category_counts = get_min_category_counts() %}
{% set spent_share_buckets = get_spent_share_buckets() %}

select
    deposit_expiration_month,
    region_name,
    region_code,
    department_name,
    department_code,
    epci_name,
    epci_code,
    city_name,
    city_code,
    municipality_label,
    municipality_code,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_expired_deposit_beneficiaries,
    cell_key_expired,
    {% for n in min_category_counts %}
        total_{{ n }}plus_category_booked_beneficiaries, cell_key_{{ n }}plus,
    {% endfor %}
    {% for bucket in spent_share_buckets %}
        total_spent_share_{{ bucket.name }}_beneficiaries,
        cell_key_spent_share_{{ bucket.name }},
    {% endfor %}
    total_actual_amount_spent_at_expiration,
    total_distinct_venue_booked_at_expiration
from {{ ref("int_kpi__diversity") }}
