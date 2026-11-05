-- Cultural partners (venues) with at least one collective template offer
-- ("offre vitrine"), by month of their first template offer and partner geography.
with
    partner_first_template_offer as (
        select
            gcp.partner_id,
            gcp.partner_region_name,
            gcp.partner_region_code,
            gcp.partner_academy_name,
            gcp.partner_department_name,
            gcp.partner_department_code,
            gcp.partner_epci_code,
            gcp.partner_city_code,
            gcp.partner_municipality_code,
            min(co.collective_offer_creation_date) as first_template_offer_creation_date
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        inner join
            {{ ref("int_global__collective_offer") }} as co
            on gcp.partner_id = co.partner_id
        where co.collective_offer_is_template
        group by
            gcp.partner_id,
            gcp.partner_region_name,
            gcp.partner_region_code,
            gcp.partner_academy_name,
            gcp.partner_department_name,
            gcp.partner_department_code,
            gcp.partner_epci_code,
            gcp.partner_city_code,
            gcp.partner_municipality_code
    ),

    monthly_new_partners as (
        select
            partner_region_name,
            partner_region_code,
            partner_academy_name,
            partner_department_name,
            partner_department_code,
            partner_epci_code,
            partner_city_code,
            partner_municipality_code,
            date_trunc(first_template_offer_creation_date, month) as partition_month,
            count(distinct partner_id) as total_new_partners_with_template_offer
        from partner_first_template_offer
        group by
            partition_month,
            partner_region_name,
            partner_region_code,
            partner_academy_name,
            partner_department_name,
            partner_department_code,
            partner_epci_code,
            partner_city_code,
            partner_municipality_code
    ),

    -- every month from the first template offer of the geography to the current
    -- month, so that the cumulative column can be read on any month
    geography_months as (
        select
            partition_month,
            partner_region_name,
            partner_region_code,
            partner_academy_name,
            partner_department_name,
            partner_department_code,
            partner_epci_code,
            partner_city_code,
            partner_municipality_code
        from
            (
                select
                    partner_region_name,
                    partner_region_code,
                    partner_academy_name,
                    partner_department_name,
                    partner_department_code,
                    partner_epci_code,
                    partner_city_code,
                    partner_municipality_code,
                    min(partition_month) as first_partition_month
                from monthly_new_partners
                group by
                    partner_region_name,
                    partner_region_code,
                    partner_academy_name,
                    partner_department_name,
                    partner_department_code,
                    partner_epci_code,
                    partner_city_code,
                    partner_municipality_code
            ) as first_months
        cross join
            unnest(
                generate_date_array(
                    first_partition_month,
                    date_trunc(current_date(), month),
                    interval 1 month
                )
            ) as partition_month
    )

select
    gm.partition_month,
    gm.partner_region_name,
    gm.partner_region_code,
    gm.partner_academy_name,
    gm.partner_department_name,
    gm.partner_department_code,
    gm.partner_epci_code,
    gm.partner_city_code,
    gm.partner_municipality_code,
    coalesce(
        mnp.total_new_partners_with_template_offer, 0
    ) as total_new_partners_with_template_offer,
    sum(coalesce(mnp.total_new_partners_with_template_offer, 0)) over (
        partition by
            gm.partner_region_code,
            gm.partner_department_code,
            gm.partner_epci_code,
            gm.partner_city_code,
            gm.partner_municipality_code
        order by gm.partition_month asc
        rows between unbounded preceding and current row
    ) as total_cumulative_partners_with_template_offer
from geography_months as gm
left join
    monthly_new_partners as mnp
    on gm.partition_month = mnp.partition_month
    and gm.partner_region_code = mnp.partner_region_code
    and gm.partner_department_code = mnp.partner_department_code
    and gm.partner_epci_code = mnp.partner_epci_code
    and gm.partner_city_code = mnp.partner_city_code
    and gm.partner_municipality_code = mnp.partner_municipality_code
where gm.partner_municipality_code is not null
