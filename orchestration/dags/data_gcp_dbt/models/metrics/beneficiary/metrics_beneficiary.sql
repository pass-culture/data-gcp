select
    partition_month,
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
    age_at_calculation,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_actual_beneficiaries,
    total_beneficiaries,
    cell_key_beneficiaries,
    cell_key_actual_beneficiaries,
    {% for breakdown in get_actual_beneficiary_breakdowns() %}
        total_actual_beneficiaries_{{ breakdown.name }},
        cell_key_actual_beneficiaries_{{ breakdown.name }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref("int_kpi__beneficiary") }}
