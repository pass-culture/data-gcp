{#
    Used individual bookings and amounts spent by the beneficiaries, by month of
    use and territory of residence (current address), all categories
    (offer_category_id = 'ALL') and by offer category, at every published
    level: DEP (every department, overseas collectivities included), REG (the
    18 regions), GROUP (METRO, DROM, COM) and NAT.

    Bookings and amounts are not counts of people and cannot be perturbed: the
    low count rule of external_reporting__beneficiary_booking applies, on the
    distinct bookers of the territory since 2022 (sum of the new bookers):
        - the 'ALL' row is masked when 1 to 4 beneficiaries account for it;
        - a category row is masked when 1 to 4 beneficiaries booked in the
          category, or when the territory has 1 to 19 bookers (category
          breakdown);
        - a parent (REG, GROUP, NAT) is also masked when its masked members
          (departments, and departments missing from the referential for NAT)
          together have 1 to 4 bookers: published, it would give them by
          subtraction.
    Masked rows are NULL on every month. Each level is computed on unmasked
    data: never sum a level to get another one.

    Same sources as exp_vidoc__booking_beneficiary before the base: bookings of
    users missing from mrt_global__user_beneficiary (deleted, suspended,
    GRANT_FREE) are left out, months from 2022 to the month before the run date.
#}
{% set last_month = (
    "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
) %}
{% set min_bookers = 5 %}
{% set min_category_breakdown_bookers = 20 %}
{% set categories = get_categories() %}
{% set category_list %}
    {% for category in categories %}
        '{{ category.name }}'{% if not loop.last %},{% endif %}
    {% endfor %}
{% endset %}

with
    bookings as (
        select
            partition_month,
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
        group by partition_month, department_name, offer_category_id
    ),

    department_cells as (
        select
            partition_month,
            department_name,
            'ALL' as offer_category_id,
            sum(total_used_bookings) as total_used_bookings,
            sum(total_used_amount) as total_used_amount
        from bookings
        group by partition_month, department_name
        union all
        select *
        from bookings
        where offer_category_id in ({{ category_list }})
    ),

    -- distinct bookers of the department since 2022, all categories and by
    -- category ('ALL' = the department)
    department_bookers as (
        select
            department_name,
            coalesce(offer_category_id, 'ALL') as offer_category_id,
            sum(total_new_bookers) as total_bookers,
            sum(total_new_category_bookers) as total_category_bookers
        from {{ ref("metrics_booking__individual_bookers") }}
        where partition_month <= {{ last_month }} and department_name is not null
        group by grouping sets ((department_name), (department_name, offer_category_id))
    ),

    -- one row per department and category: bookers of the cell (department
    -- bookers for 'ALL', category bookers otherwise) and of the department
    department_keys as (
        select
            keys.department_name,
            keys.offer_category_id,
            rd.num_dep as department_code,
            cast(cast(rd.region_code as int64) as string) as region_code,
            if(rd.region_code is null, null, rd.region_name) as region_name,
            if(
                rd.num_dep is null, null, {{ territory_group("rd.num_dep") }}
            ) as territory_group,
            coalesce(dep.total_bookers, 0) as territory_bookers,
            if(
                keys.offer_category_id = 'ALL',
                coalesce(dep.total_bookers, 0),
                coalesce(cat.total_category_bookers, 0)
            ) as cell_bookers
        from
            (
                select distinct department_name, offer_category_id from department_cells
            ) as keys
        left join
            department_bookers as dep
            on keys.department_name = dep.department_name
            and dep.offer_category_id = 'ALL'
        left join
            department_bookers as cat
            on keys.department_name = cat.department_name
            and keys.offer_category_id = cat.offer_category_id
        left join
            {{ source("seed", "region_department") }} as rd
            on keys.department_name = rd.dep_name
    ),

    -- members (departments) of every published territory. Departments missing
    -- from the referential are members of NAT only and never published
    members as (
        {% for level in ["DEP", "REG", "GROUP", "NAT"] %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                '{{ level }}' as territory_level,
                {% if level == "DEP" %}
                    department_code as territory_code,
                    department_name as territory_name,
                    region_code,
                    territory_group,
                {% elif level == "REG" %}
                    region_code as territory_code,
                    region_name as territory_name,
                    region_code,
                    territory_group,
                {% elif level == "GROUP" %}
                    territory_group as territory_code,
                    territory_group as territory_name,
                    cast(null as string) as region_code,
                    territory_group,
                {% else %}
                    'NAT' as territory_code,
                    'France' as territory_name,
                    cast(null as string) as region_code,
                    cast(null as string) as territory_group,
                {% endif %}
                department_name,
                offer_category_id,
                cell_bookers,
                territory_bookers
            from department_keys
            {% if level in ["DEP", "GROUP"] %} where territory_group is not null
            {% elif level == "REG" %} where region_code is not null
            {% endif %}
        {% endfor %}
    ),

    {% set low_count %}
        (
            cell_bookers between 1 and {{ min_bookers - 1 }}
            or (
                offer_category_id != 'ALL'
                and territory_bookers
                between 1 and {{ min_category_breakdown_bookers - 1 }}
            )
        )
    {% endset %}

    -- primary mask of the departments; departments missing from the
    -- referential are never published, so always hidden
    department_mask_0 as (
        select
            department_name,
            offer_category_id,
            region_code,
            territory_group,
            'NAT' as nation,
            cell_bookers,
            territory_group is not null as is_published,
            territory_group is null or {{ low_count }} as is_hidden
        from department_keys
    ),

    {#
        Secondary mask, one pass per parent level (region, territory group,
        France): when the hidden departments of a parent have 1 to 4 bookers
        together, the parent total would give them by subtraction. The visible
        departments of the parent with bookers are added to the hidden set from
        the smallest up, until it has at least 5 bookers. Each added department
        has 5 bookers or more, so a pass never breaks an earlier one. When no
        department can be added, the parent itself is masked (see
        territory_secret).
    #}
    {% set passes = ["region_code", "territory_group", "nation"] %}
    {% for parent in passes %}
        {% set src = "department_mask_" ~ loop.index0 %}
        {% set dst = "department_mask_" ~ loop.index %}
        {{ dst }}_hidden as (
            select
                {{ parent }} as parent,
                offer_category_id,
                sum(if(is_hidden, cell_bookers, 0)) as hidden_bookers
            from {{ src }}
            where {{ parent }} is not null
            group by {{ parent }}, offer_category_id
        ),

        {{ dst }}_candidates as (
            select
                department_name,
                offer_category_id,
                {{ parent }} as parent,
                sum(cell_bookers) over (
                    partition by {{ parent }}, offer_category_id
                    order by cell_bookers asc, department_name asc
                    rows between unbounded preceding and current row
                ) as cumulative_bookers
            from {{ src }}
            where {{ parent }} is not null and not is_hidden and cell_bookers > 0
        ),

        {{ dst }}_cut as (
            select
                hidden.parent,
                hidden.offer_category_id,
                min(
                    if(
                        hidden.hidden_bookers + candidates.cumulative_bookers
                        >= {{ min_bookers }},
                        candidates.cumulative_bookers,
                        null
                    )
                ) as cut_bookers
            from {{ dst }}_hidden as hidden
            inner join
                {{ dst }}_candidates as candidates
                on hidden.parent = candidates.parent
                and hidden.offer_category_id = candidates.offer_category_id
            where hidden.hidden_bookers between 1 and {{ min_bookers - 1 }}
            group by hidden.parent, hidden.offer_category_id
        ),

        {{ dst }} as (
            select
                src.* except (is_hidden),
                src.is_hidden
                or coalesce(
                    candidates.cumulative_bookers <= cut.cut_bookers, false
                ) as is_hidden
            from {{ src }} as src
            left join
                {{ dst }}_candidates as candidates
                on src.department_name = candidates.department_name
                and src.offer_category_id = candidates.offer_category_id
            left join
                {{ dst }}_cut as cut
                on candidates.parent = cut.parent
                and candidates.offer_category_id = cut.offer_category_id
        ),
    {% endfor %}
    {% set final_mask = "department_mask_" ~ (passes | length) %}

    territory_flags as (
        select
            members.territory_level,
            members.territory_code,
            members.offer_category_id,
            any_value(members.territory_name) as territory_name,
            any_value(members.region_code) as region_code,
            any_value(members.territory_group) as territory_group,
            sum(members.cell_bookers) as cell_bookers,
            sum(members.territory_bookers) as territory_bookers,
            -- DEP: the department is hidden; parents: bookers of the hidden
            -- members, which the parent total would give by subtraction
            logical_or(mask.is_hidden) as is_any_member_hidden,
            sum(if(mask.is_hidden, members.cell_bookers, 0)) as hidden_member_bookers
        from members
        inner join
            {{ final_mask }} as mask
            on members.department_name = mask.department_name
            and members.offer_category_id = mask.offer_category_id
        group by
            members.territory_level, members.territory_code, members.offer_category_id
    ),

    territory_secret as (
        select
            *,
            if(
                territory_level = 'DEP',
                is_any_member_hidden,
                {{ low_count }}
                or hidden_member_bookers between 1 and {{ min_bookers - 1 }}
            ) as is_low_count
        from territory_flags
    ),

    figures as (
        select
            members.territory_level,
            members.territory_code,
            cells.offer_category_id,
            cells.partition_month,
            sum(cells.total_used_bookings) as total_used_bookings,
            sum(cells.total_used_amount) as total_used_amount
        from department_cells as cells
        inner join
            members
            on cells.department_name = members.department_name
            and cells.offer_category_id = members.offer_category_id
        group by
            members.territory_level,
            members.territory_code,
            cells.offer_category_id,
            cells.partition_month
    )

select
    figures.partition_month,
    figures.territory_level,
    figures.territory_code,
    sec.territory_name,
    sec.region_code,
    sec.territory_group,
    figures.offer_category_id,
    sec.is_low_count,
    if(sec.is_low_count, null, figures.total_used_bookings) as total_used_bookings,
    if(sec.is_low_count, null, figures.total_used_amount) as total_used_amount
from figures
inner join
    territory_secret as sec
    on figures.territory_level = sec.territory_level
    and figures.territory_code = sec.territory_code
    and figures.offer_category_id = sec.offer_category_id
