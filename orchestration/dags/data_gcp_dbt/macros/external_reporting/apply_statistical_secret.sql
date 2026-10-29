{% macro apply_statistical_secret(
    kpis_cte,
    secret_models,
    rolling_months_list=[6, 48],
    kpi_attributes=[],
    zero_row_columns={}
) %}
    {#
        Mask external reporting KPIs of territories under statistical secret, add
        zero rows so that territories without data report 0 instead of nothing, and
        add display labels (label, intitule) so that reports read the table as is.

        Must be called right after a `with <kpis_cte> as (...)` CTE holding the
        external reporting rows (partition_month, updated_at, dimension_name,
        dimension_value, kpi_name, numerator, denominator, kpi, ...). It appends the
        remaining CTEs and the final select.

        Rules:
            - a territory (COM, EPCI, DEP) is secret when it is secret
              (is_*_secret_stat) at the last complete month, for any of the
              rolling windows and any of the secret models;
            - numerator, denominator and kpi are NULL on every month of a secret
              territory; REG, ACAD and NAT are never secret;
            - for every territory of the geo referential and every KPI, a zero row
              is added at the last complete month when the territory has never had
              a row for this KPI (except NAT). Territories with past rows get no zero
              row, so that cumulative / stock KPIs are never reset to 0.

        Parameters:
            kpis_cte (string): name of the CTE holding the KPI rows
            secret_models (list): statistical secret models to apply
            rolling_months_list (list): rolling windows to apply
            kpi_attributes (list): KPI level columns copied on zero rows
            zero_row_columns (dict): other columns of zero rows, name -> expression
    #}
    {% set last_month = (
        "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
    ) %},
    kpi_list as (
        select
            dimension_name,
            kpi_name
            {% for attribute in kpi_attributes %}
                , any_value({{ attribute }}) as {{ attribute }}
            {% endfor %}
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

    filled_kpis as (
        select *
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
            {% for attribute in kpi_attributes %}, kpi_list.{{ attribute }} {% endfor %}
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

    flagged_kpis as (
        select
            filled_kpis.*,
            coalesce(secret_territories.is_secret_stat, false) as is_secret_stat,
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
    )

    select
        * except (numerator, denominator, kpi, is_secret_stat, label, intitule),
        label,
        intitule,
        if(is_secret_stat, null, coalesce(numerator, 0)) as numerator,
        if(is_secret_stat, null, coalesce(denominator, 0)) as denominator,
        if(is_secret_stat, null, coalesce(kpi, 0)) as kpi,
        is_secret_stat,
        if(
            is_secret_stat, 'Données non disponibles (secret statistique)', null
        ) as secret_reason
    from flagged_kpis
{% endmacro %}
