{#
    Published territory levels of a venue-side table (bookings and amounts
    measured on the cultural partner geography), under the statistical secret of
    businesses (statistical_secret__cultural_partner_*).

    Must be called right after a `with <facts_cte> as (...)` CTE (no trailing
    comma) holding the unmasked figures at department grain, with the columns
    partition_month, <dimensions>, department_code and <measures>. It appends
    the remaining CTEs and the final select.

    Every figure is computed on unmasked data and published at its own level, so
    that no tool sums masked cells:
        - DEP: every department of the referential (COM included);
        - REG: the 18 regions (metropolitan and overseas regions with a region
          code). Overseas collectivities have no region: they are published in
          the COM group only;
        - GROUP: METRO, DROM and COM (territory_group);
        - NAT: every venue, including venues missing from the referential.

    A territory is secret when it is secret at the last complete month for any
    rolling window (6, 48 months) and on the individual or the collective side
    (partner pages show both); its figures are then NULL on every month. The
    secret model already masks a second department when a region or territory
    group total would disclose a masked one (secondary secret), and flags region
    and group totals that would still disclose it (is_region_secret_stat,
    is_territory_group_secret_stat). NAT is never secret.

    Parameters:
        facts_cte (string): name of the CTE holding the department figures
        measures (list): measure columns, summed then masked
        dimensions (list): other grouping columns (e.g. scholar_year)
#}
{% macro venue_secret_levels(facts_cte, measures, dimensions=[]) %}
    {% set last_month = (
        "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
    ) %}
    {% set dims = dimensions | join(", ") ~ (", " if dimensions else "") %}
    {% set located_dims %}
        located.partition_month,
        {% for d in dimensions %} located.{{ d }}, {% endfor %}
    {% endset %},
    secret_source as (
        {% for side in ["individual", "collective"] %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                department_code,
                region_name,
                territory_group,
                is_department_secret_stat,
                is_region_secret_stat,
                is_territory_group_secret_stat
            from {{ ref("statistical_secret__cultural_partner_" ~ side) }}
            where
                partition_month = {{ last_month }}
                and rolling_window_months_count in (6, 48)
        {% endfor %}
    ),

    secret_territories as (
        select
            'DEP' as territory_level,
            department_code as territory_code,
            logical_or(is_department_secret_stat) as is_secret
        from secret_source
        where department_code is not null
        group by department_code
        union all
        select
            'REG', cast(rd.region_code as string), logical_or(s.is_region_secret_stat)
        from secret_source as s
        inner join
            (
                select distinct region_name, cast(region_code as int64) as region_code
                from {{ source("seed", "region_department") }}
                where region_code is not null
            ) as rd
            on s.region_name = rd.region_name
        group by rd.region_code
        union all
        select 'GROUP', territory_group, logical_or(is_territory_group_secret_stat)
        from secret_source
        where territory_group is not null
        group by territory_group
    ),

    located as (
        select
            facts.*,
            rd.dep_name as department_name,
            cast(cast(rd.region_code as int64) as string) as region_code,
            if(rd.region_code is null, null, rd.region_name) as region_name,
            if(
                rd.num_dep is null, null, {{ territory_group("facts.department_code") }}
            ) as territory_group
        from {{ facts_cte }} as facts
        left join
            {{ source("seed", "region_department") }} as rd
            on facts.department_code = rd.num_dep
    ),

    levels as (
        select
            partition_month,
            {{ dims }}
            'DEP' as territory_level,
            department_code as territory_code,
            any_value(department_name) as territory_name,
            any_value(region_code) as region_code,
            any_value(territory_group) as territory_group,
            {% for m in measures %} sum({{ m }}) as {{ m }}, {% endfor %}
        from located
        where territory_group is not null
        group by {{ located_dims }} located.department_code
        union all
        select
            partition_month,
            {{ dims }}
            'REG',
            region_code,
            any_value(region_name),
            region_code,
            any_value(territory_group),
            {% for m in measures %} sum({{ m }}), {% endfor %}
        from located
        where region_code is not null
        group by {{ located_dims }} located.region_code
        union all
        select
            partition_month,
            {{ dims }}
            'GROUP',
            territory_group,
            territory_group,
            cast(null as string),
            territory_group,
            {% for m in measures %} sum({{ m }}), {% endfor %}
        from located
        where territory_group is not null
        group by {{ located_dims }} located.territory_group
        union all
        select
            partition_month,
            {{ dims }}
            'NAT',
            'NAT',
            'France',
            cast(null as string),
            cast(null as string),
            {% for m in measures %} sum({{ m }}), {% endfor %}
        from located
        group by
            located.partition_month
            {% for d in dimensions %}, located.{{ d }}{% endfor %}
    )

    select
        levels.partition_month,
        {% for d in dimensions %} levels.{{ d }}, {% endfor %}
        levels.territory_level,
        levels.territory_code,
        levels.territory_name,
        levels.region_code,
        levels.territory_group,
        coalesce(sec.is_secret, false) as is_statistic_secret,
        {% for m in measures %}
            if(coalesce(sec.is_secret, false), null, levels.{{ m }}) as {{ m }}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    from levels
    left join
        secret_territories as sec
        on levels.territory_level = sec.territory_level
        and levels.territory_code = sec.territory_code
{% endmacro %}
