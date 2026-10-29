{% macro apply_statistical_secret(
    kpis_cte,
    secret_models,
    rolling_months_list=[6, 48],
    kpi_attributes=[],
    zero_row_columns={},
    low_count_models=[],
    low_count_denominator_kpis=[],
    min_individuals=5
) %}
    {#
        Mask external reporting KPIs of territories under statistical secret, add
        zero rows so that territories without data report 0 instead of nothing, add
        the national value on every row and display labels (label, intitule) so
        that reports read the table as is, with a single source per table.

        Must be called right after a `with <kpis_cte> as (...)` CTE holding the
        external reporting rows, in this column order: partition_month,
        updated_at, dimension_name, dimension_value, kpi_name, numerator,
        denominator, kpi, <zero_row_columns>, <kpi_attributes>,
        is_secret_applicable. is_secret_applicable is true for KPIs measured on
        the cultural partner (venue) geography, the only ones subject to the
        statistical secret. It appends the remaining CTEs and the final select.

        Rules:
            - a territory (COM, EPCI, DEP) is secret when it is secret
              (is_*_secret_stat) at the last complete month, for any of the
              rolling windows and any of the secret models;
            - numerator, denominator and kpi are NULL on every month of a secret
              territory, for the KPIs subject to the secret only; REG, ACAD and NAT
              are never secret;
            - for every territory of the geo referential and every KPI, a zero row
              is added at the last complete month when the territory has never had
              a row for this KPI (except NAT). Territories with past rows get no zero
              row, so that cumulative / stock KPIs are never reset to 0;
            - national_numerator, national_denominator and national_kpi hold the NAT
              value of the same month and KPI, so that a report filtered on a
              territory still shows the national figures;
            - KPIs published at NAT but not at a territory level (e.g. coverage
              rates) get rows for every territory of that level with NULL
              numerator, denominator and kpi and is_national_only = true, so that
              their national value is available whatever the territory filter;
            - a KPI that 1 to min_individuals - 1 people of a territory account
              for (low_count_models) is NULL on every month (is_low_count), as
              amounts and booking counts cannot be blurred;
            - an average or share whose denominator counts 1 to min_individuals - 1
              people (low_count_denominator_kpis) is NULL on that month
              (is_low_count), as it would disclose individual values.

        Parameters:
            kpis_cte (string): name of the CTE holding the KPI rows
            secret_models (list): statistical secret models to apply
            rolling_months_list (list): rolling windows to apply
            kpi_attributes (list): KPI level columns copied on zero rows
            zero_row_columns (dict): other columns of zero rows, name -> expression
            low_count_models (list): models of distinct people behind each KPI
                (dimension_name, dimension_value, kpi_name, total_beneficiaries)
            low_count_denominator_kpis (list): KPI name prefixes whose denominator
                counts people (averages and shares by person)
            min_individuals (int): minimum number of people behind a published KPI
    #}
    {% set last_month = (
        "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
    ) %},
    kpi_list as (
        select
            dimension_name,
            kpi_name,
            {% for attribute in kpi_attributes %}
                any_value({{ attribute }}) as {{ attribute }},
            {% endfor %}
            logical_or(is_secret_applicable) as is_secret_applicable
        from {{ kpis_cte }}
        group by dimension_name, kpi_name
    ),

    territories as (
        select distinct dimension_name, dimension_value
        from {{ kpis_cte }}
        where dimension_value is not null
        union distinct
        select geo_level.dimension_name, geo_level.dimension_value
        from {{ ref("int_seed__geo_iris") }} as geo
        cross join
            unnest(
                [
                    struct(
                        'COM' as dimension_name,
                        geo.municipality_code as dimension_value
                    ),
                    struct('EPCI' as dimension_name, geo.epci_code as dimension_value),
                    struct(
                        'DEP' as dimension_name, geo.department_name as dimension_value
                    ),
                    struct('REG' as dimension_name, geo.region_name as dimension_value)
                ]
            ) as geo_level
        where geo_level.dimension_value is not null
    ),

    existing_kpis as (
        select distinct dimension_name, dimension_value, kpi_name from {{ kpis_cte }}
    ),

    national_only_kpis as (
        select territory_levels.dimension_name, national_kpis.kpi_name
        from (select distinct dimension_name from territories) as territory_levels
        cross join
            (
                select kpi_name from kpi_list where dimension_name = 'NAT'
            ) as national_kpis
        left join
            kpi_list
            on territory_levels.dimension_name = kpi_list.dimension_name
            and national_kpis.kpi_name = kpi_list.kpi_name
        where territory_levels.dimension_name != 'NAT' and kpi_list.kpi_name is null
    ),

    filled_kpis as (
        select *, false as is_national_only
        from {{ kpis_cte }}
        union all
        select
            {{ last_month }} as partition_month,
            timestamp('{{ ts() }}') as updated_at,
            territories.dimension_name,
            territories.dimension_value,
            kpi_list.kpi_name,
            0 as numerator,
            0 as denominator,
            0 as kpi
            {% for column, expression in zero_row_columns.items() %}
                , {{ expression }} as {{ column }}
            {% endfor %}
            {% for attribute in kpi_attributes %}
                , kpi_list.{{ attribute }}
            {% endfor %},
            kpi_list.is_secret_applicable,
            false as is_national_only
        from territories
        inner join kpi_list on territories.dimension_name = kpi_list.dimension_name
        left join
            existing_kpis
            on territories.dimension_name = existing_kpis.dimension_name
            and territories.dimension_value = existing_kpis.dimension_value
            and kpi_list.kpi_name = existing_kpis.kpi_name
        where
            existing_kpis.kpi_name is null
            -- national KPIs always exist, and are checked month over month
            and territories.dimension_name != 'NAT'
        union all
        select
            national.partition_month,
            national.updated_at,
            territories.dimension_name,
            territories.dimension_value,
            national.kpi_name,
            null as numerator,
            null as denominator,
            null as kpi
            {% for column in zero_row_columns.keys() %}
                , national.{{ column }}
            {% endfor %}
            {% for attribute in kpi_attributes %}
                , national.{{ attribute }}
            {% endfor %},
            false as is_secret_applicable,
            true as is_national_only
        from {{ kpis_cte }} as national
        inner join national_only_kpis on national.kpi_name = national_only_kpis.kpi_name
        inner join
            territories
            on national_only_kpis.dimension_name = territories.dimension_name
        where national.dimension_name = 'NAT'
    ),

    national_values as (
        select
            partition_month,
            kpi_name,
            numerator as national_numerator,
            denominator as national_denominator,
            kpi as national_kpi
        from {{ kpis_cte }}
        where dimension_name = 'NAT'
    ),

    secret_source as (
        {% for secret_model in secret_models %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                municipality_code,
                epci_code,
                department_label,
                is_municipality_secret_stat,
                is_epci_secret_stat,
                is_department_secret_stat
            from {{ ref(secret_model) }}
            where
                partition_month = {{ last_month }}
                and rolling_window_months_count
                in ({{ rolling_months_list | join(", ") }})
        {% endfor %}
    ),

    secret_territories as (
        select
            dimension_name,
            dimension_value,
            logical_or(is_secret_stat) as is_secret_stat
        from
            (
                select
                    'COM' as dimension_name,
                    municipality_code as dimension_value,
                    is_municipality_secret_stat as is_secret_stat
                from secret_source
                union all
                select
                    'EPCI' as dimension_name,
                    epci_code as dimension_value,
                    is_epci_secret_stat as is_secret_stat
                from secret_source
                where epci_code is not null
                union all
                -- DEP dimension_value is the department name in external reporting
                select
                    'DEP' as dimension_name,
                    department_label as dimension_value,
                    is_department_secret_stat as is_secret_stat
                from secret_source
                where department_label is not null
            )
        group by dimension_name, dimension_value
    ),

    territory_labels as (
        select
            'EPCI' as dimension_name, epci_code as dimension_value, epci_label as label
        from {{ ref("int_seed__geo_iris") }}
        where epci_code is not null
        qualify row_number() over (partition by epci_code order by epci_label) = 1
        union all
        select
            'COM' as dimension_name,
            municipality_code as dimension_value,
            municipality_label as label
        from {{ ref("int_seed__geo_iris") }}
        where municipality_code is not null
        qualify
            row_number() over (
                partition by municipality_code order by municipality_label
            )
            = 1
    ),

    low_count_kpis as (
        {% for low_count_model in low_count_models %}
            {% if not loop.first %}
                union distinct
            {% endif %}
            select dimension_name, dimension_value, kpi_name
            from {{ ref(low_count_model) }}
            where total_beneficiaries between 1 and {{ min_individuals - 1 }}
        {% else %}
            select
                cast(null as string) as dimension_name,
                cast(null as string) as dimension_value,
                cast(null as string) as kpi_name
            from unnest([1])
            where false
        {% endfor %}
    ),

    flagged_kpis as (
        select
            filled_kpis.*,
            filled_kpis.dimension_name != 'NAT'
            and (
                low_count_kpis.kpi_name is not null
                {% for prefix in low_count_denominator_kpis %}
                    or (
                        starts_with(filled_kpis.kpi_name, '{{ prefix }}')
                        and filled_kpis.denominator
                        between 1 and {{ min_individuals - 1 }}
                    )
                {% endfor %}
            ) as is_low_count,
            filled_kpis.is_secret_applicable
            and coalesce(secret_territories.is_secret_stat, false) as is_secret_stat,
            case
                when filled_kpis.dimension_name in ('EPCI', 'COM')
                then territory_labels.label
                else filled_kpis.dimension_value
            end as label,
            case
                when filled_kpis.dimension_name = 'NAT'
                then 'Rapport National'
                when filled_kpis.dimension_name in ('EPCI', 'COM')
                then
                    concat(
                        territory_labels.label, ' (', filled_kpis.dimension_value, ')'
                    )
                else filled_kpis.dimension_value
            end as intitule
        from filled_kpis
        left join
            secret_territories
            on filled_kpis.dimension_name = secret_territories.dimension_name
            and filled_kpis.dimension_value = secret_territories.dimension_value
        left join
            territory_labels
            on filled_kpis.dimension_name = territory_labels.dimension_name
            and filled_kpis.dimension_value = territory_labels.dimension_value
        left join
            low_count_kpis
            on filled_kpis.dimension_name = low_count_kpis.dimension_name
            and filled_kpis.dimension_value = low_count_kpis.dimension_value
            and filled_kpis.kpi_name = low_count_kpis.kpi_name
    )

    select
        flagged_kpis.* except (
            numerator,
            denominator,
            kpi,
            is_secret_applicable,
            is_national_only,
            is_secret_stat,
            is_low_count,
            label,
            intitule
        ),
        flagged_kpis.label,
        flagged_kpis.intitule,
        if(
            flagged_kpis.is_secret_stat
            or flagged_kpis.is_low_count
            or flagged_kpis.is_national_only,
            null,
            coalesce(flagged_kpis.numerator, 0)
        ) as numerator,
        if(
            flagged_kpis.is_secret_stat
            or flagged_kpis.is_low_count
            or flagged_kpis.is_national_only,
            null,
            coalesce(flagged_kpis.denominator, 0)
        ) as denominator,
        if(
            flagged_kpis.is_secret_stat
            or flagged_kpis.is_low_count
            or flagged_kpis.is_national_only,
            null,
            coalesce(flagged_kpis.kpi, 0)
        ) as kpi,
        national_values.national_numerator,
        national_values.national_denominator,
        national_values.national_kpi,
        flagged_kpis.is_secret_stat,
        flagged_kpis.is_low_count,
        flagged_kpis.is_national_only,
        case
            when flagged_kpis.is_secret_stat
            then 'Données non disponibles (secret statistique)'
            when flagged_kpis.is_low_count
            then 'Données non disponibles (moins de 5 bénéficiaires)'
        end as secret_reason
    from flagged_kpis
    left join
        national_values
        on flagged_kpis.partition_month = national_values.partition_month
        and flagged_kpis.kpi_name = national_values.kpi_name
{% endmacro %}
