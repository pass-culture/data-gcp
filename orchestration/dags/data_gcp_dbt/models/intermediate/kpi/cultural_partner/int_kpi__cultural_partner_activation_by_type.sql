{% set partner_types = get_partner_types() %}

with
    union_partner_types as (
        {% for partner_type in partner_types %}
            select
                partition_month,
                partner_region_name,
                partner_region_code,
                partner_department_name,
                partner_department_code,
                partner_epci_code,
                partner_city_code,
                '{{ partner_type.name }}' as partner_type,
                venue_id,
                days_since_last_indiv_bookable_date,
                days_since_last_collective_bookable_date
            from {{ ref("int_kpi__cultural_partner_activity_base") }}
            where {{ partner_type.condition }}

            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    ),

    daily_aggregated_kpis as (
        select
            partner_region_name,
            partner_region_code,
            partner_department_name,
            partner_department_code,
            partner_epci_code,
            partner_city_code,
            partner_type,
            partition_month,
            count(
                distinct case
                    when days_since_last_indiv_bookable_date <= 365 then venue_id
                end
            ) as total_active_partners_individual,
            count(
                distinct case
                    when days_since_last_collective_bookable_date <= 365 then venue_id
                end
            ) as total_active_partners_collective,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date <= 365
                        or days_since_last_collective_bookable_date <= 365
                    then venue_id
                end
            ) as total_active_partners_global,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date <= 365
                        and days_since_last_collective_bookable_date <= 365
                    then venue_id
                end
            ) as total_active_partners_dual_part,
            count(
                distinct case
                    when days_since_last_indiv_bookable_date >= 0 then venue_id
                end
            ) as total_cumulative_activated_partners_individual,
            count(
                distinct case
                    when days_since_last_collective_bookable_date >= 0 then venue_id
                end
            ) as total_cumulative_activated_partners_collective,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date >= 0
                        or days_since_last_collective_bookable_date >= 0
                    then venue_id
                end
            ) as total_cumulative_activated_partners_global,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date is not null
                        and days_since_last_collective_bookable_date is null
                    then venue_id
                end
            ) as total_cumulative_activated_partners_individual_only,
            count(
                distinct case
                    when
                        days_since_last_collective_bookable_date is not null
                        and days_since_last_indiv_bookable_date is null
                    then venue_id
                end
            ) as total_cumulative_activated_partners_collective_only,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date >= 0
                        and days_since_last_collective_bookable_date >= 0
                    then venue_id
                end
            ) as total_cumulative_activated_partners_dual_part
        from union_partner_types
        group by
            partition_month,
            partner_region_name,
            partner_region_code,
            partner_department_name,
            partner_department_code,
            partner_epci_code,
            partner_city_code,
            partner_type
    )

select
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_epci_code,
    partner_city_code,
    partner_type,
    total_active_partners_individual,
    total_active_partners_collective,
    total_active_partners_global,
    total_active_partners_dual_part,
    total_cumulative_activated_partners_individual,
    total_cumulative_activated_partners_collective,
    total_cumulative_activated_partners_global,
    total_cumulative_activated_partners_individual_only,
    total_cumulative_activated_partners_collective_only,
    total_cumulative_activated_partners_dual_part
from daily_aggregated_kpis
where partner_city_code is not null
