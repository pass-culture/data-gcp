-- Same sources and rules as external_reporting__eac_institution and
-- external_reporting__eac_credit_usage (taux_d_utilisation_du_credit).
-- Institution (school) geography. Counts of institutions and amounts of credit
-- are not data about people: no perturbation, no statistical secret.
-- Complete months only.
with
    activity as (
        select
            partition_month,
            scholar_year,
            institution_region_name as region_name,
            institution_academy_name as academy_name,
            institution_department_name as department_name,
            sum(total_institutions) as total_institutions,
            sum(total_engaged_institutions) as total_engaged_institutions,
            sum(total_ever_credited_institutions) as total_ever_credited_institutions,
            sum(total_deposit) as total_deposit
        from {{ ref("metrics_institution__activity") }}
        where partition_month < date_trunc(current_date(), month)
        group by
            partition_month, scholar_year, region_name, academy_name, department_name
    ),

    -- credit engaged (confirmed bookings, by month of booking creation) and
    -- reimbursed (by month of use), cumulated since the start of the scholar year
    bookings as (
        select
            partition_month,
            scholar_year,
            institution_department_name as department_name,
            sum(cumulative_total_collective_amount_spent) as cumulative_amount_spent,
            sum(
                cumulative_total_reimbursed_collective_amount
            ) as cumulative_reimbursed_amount
        from {{ ref("metrics_booking__collective_institution") }}
        where partition_month < date_trunc(current_date(), month)
        group by partition_month, scholar_year, department_name
    ),

    -- full join: bookings of institutions without a known department are kept on
    -- a row with no geography, so that the national sums match the source
    joined as (
        select
            activity.region_name,
            activity.academy_name,
            coalesce(
                activity.partition_month, bookings.partition_month
            ) as partition_month,
            coalesce(activity.scholar_year, bookings.scholar_year) as scholar_year,
            coalesce(
                activity.department_name, bookings.department_name
            ) as department_name,
            coalesce(activity.total_institutions, 0) as total_institutions,
            coalesce(
                activity.total_engaged_institutions, 0
            ) as total_engaged_institutions,
            coalesce(
                activity.total_ever_credited_institutions, 0
            ) as total_ever_credited_institutions,
            coalesce(activity.total_deposit, 0) as total_deposit,
            coalesce(bookings.cumulative_amount_spent, 0) as cumulative_amount_spent,
            coalesce(
                bookings.cumulative_reimbursed_amount, 0
            ) as cumulative_reimbursed_amount
        from activity
        full outer join
            bookings
            on activity.partition_month = bookings.partition_month
            and activity.scholar_year = bookings.scholar_year
            and coalesce(activity.department_name, '')
            = coalesce(bookings.department_name, '')
    )

select
    joined.partition_month,
    joined.scholar_year,
    joined.region_name,
    joined.academy_name,
    rd.num_dep as department_code,
    joined.department_name,
    joined.total_institutions,
    joined.total_engaged_institutions,
    joined.total_ever_credited_institutions,
    joined.total_deposit,
    joined.cumulative_amount_spent,
    joined.cumulative_reimbursed_amount,
    -- codes from the department name, -1 for the overseas collectivities with
    -- no region code, as in metrics_student__coverage
    if(
        joined.department_name is null, null, coalesce(rd.region_code, -1)
    ) as region_code
from joined
left join
    {{ source("seed", "region_department") }} as rd
    on joined.department_name = rd.dep_name
