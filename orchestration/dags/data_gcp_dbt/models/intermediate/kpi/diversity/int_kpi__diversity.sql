{% set min_category_counts = get_min_category_counts() %}
{% set spent_share_buckets = get_spent_share_buckets() %}

with
    final_data as (
        select
            oc.user_region_name as region_name,
            rd.region_code,
            oc.user_department_name as department_name,
            oc.user_department_code as department_code,
            oc.user_epci as epci_name,
            oc.user_epci_code as epci_code,
            oc.user_city as city_name,
            oc.user_city_code as city_code,
            oc.user_municipality_label as municipality_label,
            oc.user_municipality_code as municipality_code,
            oc.user_is_in_qpv as is_in_qpv,
            oc.user_macro_density_label as macro_density_label,
            oc.user_density_label as micro_density_label,
            date_trunc(
                date(oc.user_expiration_month), month
            ) as deposit_expiration_month,
            coalesce(sum(oc.total_users), 0) as total_expired_deposit_beneficiaries,
            mod(abs(sum(oc.cell_key_users)), 256) as cell_key_expired,
            {% for n in min_category_counts %}
                coalesce(
                    sum(oc.total_{{ n }}_category_booked_users), 0
                ) as total_{{ n }}plus_category_booked_beneficiaries,
                coalesce(
                    mod(abs(sum(oc.cell_key_{{ n }}_category)), 256), 0
                ) as cell_key_{{ n }}plus,
            {% endfor %}
            {% for bucket in spent_share_buckets %}
                coalesce(
                    sum(oc.total_spent_share_{{ bucket.name }}_users), 0
                ) as total_spent_share_{{ bucket.name }}_beneficiaries,
                coalesce(
                    mod(abs(sum(oc.cell_key_spent_share_{{ bucket.name }})), 256), 0
                ) as cell_key_spent_share_{{ bucket.name }},
            {% endfor %}
            coalesce(
                sum(oc.total_amount_spent), 0
            ) as total_actual_amount_spent_at_expiration,
            coalesce(
                sum(oc.total_venue_id_booked), 0
            ) as total_distinct_venue_booked_at_expiration
        from {{ ref("mrt_native__outgoing_cohort") }} as oc
        inner join
            {{ ref("region_department") }} as rd on oc.user_department_code = rd.num_dep
        group by
            date_trunc(date(oc.user_expiration_month), month),
            oc.user_region_name,
            rd.region_code,
            oc.user_department_name,
            oc.user_department_code,
            oc.user_epci,
            oc.user_epci_code,
            oc.user_city,
            oc.user_city_code,
            oc.user_municipality_label,
            oc.user_municipality_code,
            oc.user_is_in_qpv,
            oc.user_macro_density_label,
            oc.user_density_label
    )

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
from final_data
