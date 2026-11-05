-- noqa: disable=all
{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{#
    Schools (EPLE) credited and engaged in EAC, rolled up from
    metrics_institution__activity. A KPI is only reported where it has a base:
    credited schools of the scholar year for the participation rate, schools
    ever credited for the count since the generalisation.
#}
select
    partition_month,
    timestamp("{{ ts() }}") as updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    denominator,
    safe_divide(numerator, denominator) as kpi
from
    (
        {% for dim in get_dimensions("institution", "academic_extended") %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                if(
                    sum(total_institutions) > 0, sum(total_engaged_institutions), null
                ) as taux_participation_eac_eple_numerator,
                nullif(
                    sum(total_institutions), 0
                ) as taux_participation_eac_eple_denominator,
                nullif(
                    sum(total_ever_credited_institutions), 0
                ) as total_etablissements_credites_depuis_generalisation_numerator,
                if(
                    sum(total_ever_credited_institutions) > 0, 1, null
                ) as total_etablissements_credites_depuis_generalisation_denominator
            from {{ ref("metrics_institution__activity") }}
            {% if is_incremental() %}
                where
                    partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    ) unpivot (
        (numerator, denominator) for kpi_name in (
            (
                taux_participation_eac_eple_numerator,
                taux_participation_eac_eple_denominator
            ) as 'taux_participation_eac_eple',
            (
                total_etablissements_credites_depuis_generalisation_numerator,
                total_etablissements_credites_depuis_generalisation_denominator
            ) as 'total_etablissements_credites_depuis_generalisation'
        )
    )
