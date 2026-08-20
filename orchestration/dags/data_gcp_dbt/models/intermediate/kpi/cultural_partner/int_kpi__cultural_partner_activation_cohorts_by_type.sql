{% set partner_types = get_partner_types() %}
{% set partner_kpi_start_date = "2022-01-01" %}

with
    first_consultation as (
        select
            o.offerer_id, min(nc.consultation_date) as offerer_first_consultation_date
        from {{ ref("int_firebase__native_consultation") }} as nc
        inner join {{ ref("int_global__offer") }} as o on nc.offer_id = o.offer_id
        where nc.consultation_date >= '{{ partner_kpi_start_date }}'
        group by o.offerer_id
    ),

    offerer_first_venue as (
        select
            gcp.offerer_id,
            gcp.partner_city_code,
            gcp.partner_epci_code,
            gcp.partner_department_code,
            gcp.partner_department_name,
            gcp.partner_region_name,
            gcp.partner_region_code,
            gcp.partner_type,
            gof.offerer_creation_date,
            gof.is_reference_adage,
            date(gof.dms_submitted_at) as first_adage_application_date,
            fc.offerer_first_consultation_date,
            -- null-safe earliest date across bookable and creation dates
            least(
                coalesce(
                    gof.first_individual_bookable_offer_date,
                    gof.first_individual_offer_creation_date
                ),
                coalesce(
                    gof.first_individual_offer_creation_date,
                    gof.first_individual_bookable_offer_date
                )
            ) as first_individual_offer_date,
            gof.first_collective_offer_creation_date
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        inner join {{ ref("mrt_global__offerer") }} as gof using (offerer_id)
        left join first_consultation as fc using (offerer_id)
        where gof.offerer_creation_date > '{{ partner_kpi_start_date }}'
        qualify
            row_number() over (
                partition by gcp.offerer_id order by gcp.partner_creation_date asc
            )
            = 1  -- keep earliest venue per offerer
    ),

    time_to_activation as (
        select
            offerer_id,
            partner_city_code,
            partner_epci_code,
            partner_department_code,
            partner_department_name,
            partner_region_name,
            partner_region_code,
            partner_type,
            offerer_creation_date,
            case
                when is_reference_adage and first_adage_application_date is null
                then 0
                else
                    date_diff(
                        least(
                            coalesce(
                                first_adage_application_date,
                                offerer_first_consultation_date
                            ),
                            coalesce(
                                offerer_first_consultation_date,
                                first_adage_application_date
                            )
                        ),
                        offerer_creation_date,
                        day
                    )
            end as days_to_consultation_or_adage,
            case
                when is_reference_adage and first_adage_application_date is null
                then 0
                else
                    date_diff(
                        least(
                            coalesce(
                                first_adage_application_date,
                                first_individual_offer_date
                            ),
                            coalesce(
                                first_individual_offer_date,
                                first_adage_application_date
                            )
                        ),
                        offerer_creation_date,
                        day
                    )
            end as days_to_offer_or_adage,
            date_diff(
                least(
                    coalesce(
                        first_collective_offer_creation_date,
                        first_individual_offer_date
                    ),
                    coalesce(
                        first_individual_offer_date,
                        first_collective_offer_creation_date
                    )
                ),
                offerer_creation_date,
                day
            ) as days_to_global,
            date_diff(
                first_individual_offer_date, offerer_creation_date, day
            ) as days_to_individual,
            date_diff(
                first_collective_offer_creation_date, offerer_creation_date, day
            ) as days_to_collective
        from offerer_first_venue
        where offerer_creation_date <= date_sub(current_date(), interval 30 day)
    ),

    union_partner_types as (
        {% for partner_type in partner_types %}
            select
                offerer_id,
                partner_city_code,
                partner_epci_code,
                partner_department_code,
                partner_department_name,
                partner_region_name,
                partner_region_code,
                '{{ partner_type.name }}' as partner_type,
                offerer_creation_date,
                days_to_consultation_or_adage,
                days_to_offer_or_adage,
                days_to_global,
                days_to_individual,
                days_to_collective
            from time_to_activation
            where {{ partner_type.condition }}

            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    )

select
    date_trunc(offerer_creation_date, month) as partition_month,
    partner_city_code,
    partner_epci_code,
    partner_department_code,
    partner_department_name,
    partner_region_name,
    partner_region_code,
    partner_type,
    count(distinct offerer_id) as total_offerers_created_by_cohort,
    count(
        distinct case when days_to_consultation_or_adage <= 30 then offerer_id end
    ) as total_activated_offerer_consultation_or_adage_30d_by_cohort,
    count(
        distinct case when days_to_offer_or_adage <= 30 then offerer_id end
    ) as total_activated_offerer_offer_or_adage_30d_by_cohort,
    count(
        distinct case when days_to_global <= 30 then offerer_id end
    ) as total_activated_offerer_global_30d_by_cohort,
    count(
        distinct case when days_to_individual <= 30 then offerer_id end
    ) as total_activated_offerer_individual_30d_by_cohort,
    count(
        distinct case when days_to_collective <= 30 then offerer_id end
    ) as total_activated_offerer_collective_30d_by_cohort
from union_partner_types
group by
    date_trunc(offerer_creation_date, month),
    partner_city_code,
    partner_epci_code,
    partner_department_code,
    partner_department_name,
    partner_region_name,
    partner_region_code,
    partner_type
