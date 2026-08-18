-- Offerer-level cohort activation metrics at 30-day threshold, by first-venue
-- geography.
{% set partner_kpi_start_date = "2022-01-01" %}

with
    -- First individual offer consultation date per offerer
    first_consultation as (
        select offerer_id, min(event_date) as offerer_first_consultation_date
        from {{ ref("mrt_native__daily_offer_consultation") }}
        where event_name = 'ConsultOffer' and user_role = 'Bénéficiaire'
        group by offerer_id
    ),

    -- First venue per offerer, with geography and all relevant activation dates
    offerer_first_venue as (
        select
            gcp.offerer_id,
            gcp.partner_city_code,
            gcp.partner_epci_code,
            gcp.partner_department_code,
            gcp.partner_department_name,
            gcp.partner_region_name,
            gcp.partner_region_code,
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

    -- Days from registration to each activation event type per offerer
    time_to_activation as (
        select
            offerer_id,
            partner_city_code,
            partner_epci_code,
            partner_department_code,
            partner_department_name,
            partner_region_name,
            partner_region_code,
            offerer_creation_date,
            -- 0 if already Adage-referenced without a DMS application, else days to
            -- first consultation or Adage DMS
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
            -- 0 if already Adage-referenced without a DMS application, else days to
            -- first individual offer creation or Adage DMS
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
            ) as days_to_any,
            date_diff(
                first_individual_offer_date, offerer_creation_date, day
            ) as days_to_individual,
            date_diff(
                first_collective_offer_creation_date, offerer_creation_date, day
            ) as days_to_collective
        from offerer_first_venue
        where offerer_creation_date <= date_sub(current_date(), interval 30 day)  -- exclude offerers not yet 30 days old
    )

select
    date_trunc(offerer_creation_date, month) as creation_month,
    partner_city_code,
    partner_epci_code,
    partner_department_code,
    partner_department_name,
    partner_region_name,
    partner_region_code,
    count(distinct offerer_id) as total_offerers_created_by_cohort,
    count(
        distinct case when days_to_consultation_or_adage <= 30 then offerer_id end
    ) as total_activated_offerer_consultation_or_adage_30d_by_cohort,
    count(
        distinct case when days_to_offer_or_adage <= 30 then offerer_id end
    ) as total_activated_offerer_offer_or_adage_30d_by_cohort,
    count(
        distinct case when days_to_any <= 30 then offerer_id end
    ) as total_activated_offerer_any_30d_by_cohort,
    count(
        distinct case when days_to_individual <= 30 then offerer_id end
    ) as total_activated_offerer_individual_30d_by_cohort,
    count(
        distinct case when days_to_collective <= 30 then offerer_id end
    ) as total_activated_offerer_collective_30d_by_cohort
from time_to_activation
group by
    date_trunc(offerer_creation_date, month),
    partner_city_code,
    partner_epci_code,
    partner_department_code,
    partner_department_name,
    partner_region_name,
    partner_region_code
