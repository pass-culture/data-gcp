{#
    Used individual bookings and amounts spent by the beneficiaries, by month
    of use, region and department of residence (current address), all
    categories (offer_category_id = 'ALL') and by offer category. Same rules as
    external_reporting__beneficiary_booking and external_reporting__individual:
    - bookings of users missing from mrt_global__user_beneficiary (deleted,
      suspended, GRANT_FREE) are left out, months from 2022 to the month
      before the run date;
    - low count rule of external_reporting__beneficiary_booking_bookers, on
      the distinct bookers of the department since 2022 (sum of the new
      bookers): the 'ALL' row is masked when 1 to 4 beneficiaries account for
      it, a category row when 1 to 4 beneficiaries booked in the category or
      when the department has 1 to 19 bookers (category breakdown); masked
      rows are NULL on every month.
#}
{% set last_month = (
    "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
) %}
{% set min_bookers = 5 %}
{% set min_category_breakdown_bookers = 20 %}
{% set categories = get_categories() %}

with
    bookings as (
        select
            partition_month,
            region_name,
            department_name,
            offer_category_id,
            sum(total_used_bookings) as total_used_bookings,
            sum(total_used_amount) as total_used_amount
        from {{ ref("metrics_booking__individual") }}
        where
            user_is_beneficiary
            and total_used_bookings > 0
            and department_name is not null
            and partition_month between date('2022-01-01') and {{ last_month }}
        group by partition_month, region_name, department_name, offer_category_id
    ),

    cells as (
        select
            partition_month,
            region_name,
            department_name,
            'ALL' as offer_category_id,
            sum(total_used_bookings) as total_used_bookings,
            sum(total_used_amount) as total_used_amount
        from bookings
        group by partition_month, region_name, department_name
        union all
        select *
        from bookings
        where
            offer_category_id in (
                {% for category in categories %}
                    '{{ category.name }}'{% if not loop.last %},{% endif %}
                {% endfor %}
            )
    ),

    -- distinct bookers since 2022: the department (offer_category_id null) and
    -- each category of the department
    department_bookers as (
        select
            department_name,
            offer_category_id,
            sum(total_new_bookers) as total_bookers,
            sum(total_new_category_bookers) as total_category_bookers
        from {{ ref("metrics_booking__individual_bookers") }}
        where partition_month <= {{ last_month }} and department_name is not null
        group by grouping sets ((department_name), (department_name, offer_category_id))
    ),

    low_count_cells as (
        select
            cells.department_name,
            cells.offer_category_id,
            if(
                cells.offer_category_id = 'ALL',
                dep.total_bookers between 1 and {{ min_bookers - 1 }},
                coalesce(cat.total_category_bookers, 0)
                between 1 and {{ min_bookers - 1 }}
                or dep.total_bookers
                between 1 and {{ min_category_breakdown_bookers - 1 }}
            ) as is_low_count
        from (select distinct department_name, offer_category_id from cells) as cells
        left join
            department_bookers as dep
            on cells.department_name = dep.department_name
            and dep.offer_category_id is null
        left join
            department_bookers as cat
            on cells.department_name = cat.department_name
            and cells.offer_category_id = cat.offer_category_id
    )

select
    cells.partition_month,
    cells.region_name,
    rd.region_code,
    cells.department_name,
    rd.num_dep as department_code,
    cells.offer_category_id,
    coalesce(lc.is_low_count, false) as is_low_count,
    if(
        coalesce(lc.is_low_count, false), null, cells.total_used_bookings
    ) as total_used_bookings,
    if(
        coalesce(lc.is_low_count, false), null, cells.total_used_amount
    ) as total_used_amount
from cells
left join
    low_count_cells as lc
    on cells.department_name = lc.department_name
    and cells.offer_category_id = lc.offer_category_id
left join {{ ref("region_department") }} as rd on cells.department_name = rd.dep_name
