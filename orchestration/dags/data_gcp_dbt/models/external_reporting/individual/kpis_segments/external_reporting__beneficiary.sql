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
    Beneficiaries at the end of the month (user geography), rolled up from
    metrics_beneficiary. Beneficiary counts are blurred with the cell key
    method (see statistical_confidentiality doc): every count carries the cell
    key of the beneficiaries it counts (the key of a union of cells is the sum
    of their keys modulo 256), and is perturbed so that no published count lies
    between 1 and 4.

    The measures of base_measures are not perturbed here: at NAT, REG and DEP they
    are sums of the perturbed cells of exp_base__beneficiary, so that they match
    the vidoc and data.gouv.fr exports. Other measures (breakdowns of the active
    beneficiaries) and the EPCI and COM levels are perturbed here.

    measures: name -> (count, cell key) summed over metrics_beneficiary rows.
#}
{% set measures = {
    "total_beneficiaries": [
        "total_beneficiaries",
        "cell_key_beneficiaries",
    ],
    "actual_beneficiaries": [
        "total_actual_beneficiaries",
        "cell_key_actual_beneficiaries",
    ],
    "qpv": [
        "if(is_in_qpv, total_actual_beneficiaries, 0)",
        "if(is_in_qpv, cell_key_actual_beneficiaries, 0)",
    ],
    "rural": [
        "if(macro_density_label = 'rural', total_actual_beneficiaries, 0)",
        "if(macro_density_label = 'rural', cell_key_actual_beneficiaries, 0)",
    ],
} %}
{#
    base_measures: measure -> expression on exp_base__beneficiary (perturbed cells),
    summed at the base_dimensions levels.
#}
{% set base_measures = {
    "total_beneficiaries": "total_beneficiaries",
    "actual_beneficiaries": "total_actual_beneficiaries",
    "qpv": "if(is_in_qpv, total_actual_beneficiaries, 0)",
    "rural": "if(macro_density_label = 'rural', total_actual_beneficiaries, 0)",
} %}
{% set base_dimensions = ["NAT", "REG", "DEP"] %}
{% for breakdown in get_actual_beneficiary_breakdowns() %}
    {% do measures.update(
        {
            breakdown.name: [
                "total_actual_beneficiaries_" ~ breakdown.name,
                "cell_key_actual_beneficiaries_" ~ breakdown.name,
            ]
        }
    ) %}
{% endfor %}
{#
    kpis: kpi_name -> (numerator measure, denominator measure or none for a
    count). Shares of the active beneficiaries are only published where there
    is at least one active beneficiary.
#}
{% set kpis = {
    "beneficiaire_actuel": ["actual_beneficiaries", none],
    "beneficiaire_total": ["total_beneficiaries", none],
    "pct_beneficiaire_actuel_qpv": ["qpv", "actual_beneficiaries"],
    "pct_beneficiaire_actuel_rural": ["rural", "actual_beneficiaries"],
    "pct_beneficiaire_actuel_non_scolarise": [
        "not_in_education",
        "actual_beneficiaries",
    ],
} %}
{% for activity in get_activity_list() %}
    {% do kpis.update(
        {
            "pct_beneficiaire_actuel_"
            ~ activity.value: [
                "activity_" ~ activity.value,
                "actual_beneficiaries",
            ]
        }
    ) %}
{% endfor %}
{% do kpis.update(
    {
        "beneficiaire_actuel_homme": ["male", none],
        "beneficiaire_actuel_femme": ["female", none],
        "beneficiaire_actuel_sans_genre": ["without_civility", none],
    }
) %}
{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "region_name"},
    {"name": "DEP", "value_expr": "department_name"},
    {"name": "EPCI", "value_expr": "epci_code"},
    {"name": "COM", "value_expr": "municipality_code"},
] %}

with
    aggregated_metrics as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                {% for measure, columns in measures.items() %}
                    sum({{ columns[0] }}) as {{ measure }},
                    mod(sum({{ columns[1] }}), 256) as {{ measure }}_cell_key
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from {{ ref("metrics_beneficiary") }}
            where
                {{ dim.value_expr }} is not null
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    ),

    aggregated_base as (
        {% for dim in dimensions if dim.name in base_dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                {% for measure, expr in base_measures.items() %}
                    sum({{ expr }}) as {{ measure }}_base
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from {{ ref("exp_base__beneficiary") }}
            where
                {{ dim.value_expr }} is not null
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    ),

    aggregated as (
        select
            aggregated_metrics.*,
            -- the base leaves out all-zero cells: a NAT, REG or DEP value missing
            -- from it is 0, never a value perturbed here
            {% for measure in base_measures %}
                if(
                    aggregated_metrics.dimension_name in (
                        {% for d in base_dimensions %}
                            '{{ d }}'{% if not loop.last %},{% endif %}
                        {% endfor %}
                    ),
                    coalesce(aggregated_base.{{ measure }}_base, 0),
                    null
                ) as {{ measure }}_base
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from aggregated_metrics
        left join
            aggregated_base
            on aggregated_metrics.partition_month = aggregated_base.partition_month
            and aggregated_metrics.dimension_name = aggregated_base.dimension_name
            and aggregated_metrics.dimension_value = aggregated_base.dimension_value
    ),

    unified_metrics as (
        select
            aggregated.partition_month,
            aggregated.dimension_name,
            aggregated.dimension_value,
            kpi.*
        from aggregated
        cross join
            unnest(
                [
                    {% for kpi_name, kpi_measures in kpis.items() %}
                        struct(
                            "{{ kpi_name }}" as kpi_name,
                            {{ kpi_measures[0] }} as numerator,
                            {{ kpi_measures[1] or 1 }} as denominator,
                            {{ kpi_measures[0] }}_cell_key as numerator_cell_key,
                            {% if kpi_measures[1] %} {{ kpi_measures[1] }}_cell_key
                            {% else %} cast(null as int64)
                            {% endif %} as denominator_cell_key,
                            {% if kpi_measures[0] in base_measures %}
                                {{ kpi_measures[0] }}_base
                            {% else %} cast(null as int64)
                            {% endif %} as numerator_base,
                            {% if kpi_measures[1] in base_measures %}
                                {{ kpi_measures[1] }}_base
                            {% else %} cast(null as int64)
                            {% endif %} as denominator_base
                        )
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                ]
            ) as kpi
        where kpi.kpi_name = "beneficiaire_total" or aggregated.actual_beneficiaries > 0
    ),

    perturbed_metrics as (
        select
            unified_metrics.partition_month,
            unified_metrics.dimension_name,
            unified_metrics.dimension_value,
            unified_metrics.kpi_name,
            -- base measures are already perturbed: sums of exp_base__beneficiary cells
            coalesce(
                unified_metrics.numerator_base,
                greatest(
                    unified_metrics.numerator + coalesce(pt_num.perturbation, 0), 0
                )
            ) as numerator,
            case
                when unified_metrics.denominator_cell_key is null
                then unified_metrics.denominator
                when unified_metrics.denominator_base is not null
                then unified_metrics.denominator_base
                else
                    greatest(
                        unified_metrics.denominator + coalesce(pt_den.perturbation, 0),
                        0
                    )
            end as denominator,
            unified_metrics.denominator_cell_key is not null as is_share
        from unified_metrics
        left join
            {{ ref("perturbation_table__individual") }} as pt_num
            on unified_metrics.numerator between pt_num.count_min and pt_num.count_max
            and unified_metrics.numerator_cell_key
            between pt_num.cell_key_min and pt_num.cell_key_max
        left join
            {{ ref("perturbation_table__individual") }} as pt_den
            on unified_metrics.denominator between pt_den.count_min and pt_den.count_max
            and unified_metrics.denominator_cell_key
            between pt_den.cell_key_min and pt_den.cell_key_max
    )

-- Shares count a sub-cohort of the denominator: perturbations are independent, so
-- the perturbed numerator is capped at the perturbed denominator (shares <= 100%)
select
    partition_month,
    timestamp("{{ ts() }}") as updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    coalesce(if(is_share, least(numerator, denominator), numerator), 0) as numerator,
    coalesce(denominator, 0) as denominator,
    coalesce(
        safe_divide(
            if(is_share, least(numerator, denominator), numerator), denominator
        ),
        0
    ) as kpi
from perturbed_metrics
