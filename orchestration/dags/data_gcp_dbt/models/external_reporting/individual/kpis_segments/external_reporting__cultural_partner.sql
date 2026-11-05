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
    Active (bookable offer in the last 365 days at the end of the month) and
    cumulative activated cultural partners (venues), by partner geography.
    Venue counts are additive across geographic cells (a venue has one
    location), so every level is a sum of the metrics rows.
    EPN entities are not counted yet, pending alignment on their geography.
#}
{% set dimensions_geo = get_dimensions("partner", "geo_full") %}
{% set partner_types = get_partner_types() %}
{% set kpis = {
    "nombre_total_de_partenaire_actif_indiv_sans_collectif": "total_active_partners_individual_only",
    "nombre_total_de_partenaire_actif_collectif_sans_indiv": "total_active_partners_collective_only",
    "nombre_total_de_partenaire_actif_individuel": "total_active_partners_individual",
    "nombre_total_de_partenaire_actif_collectif": "total_active_partners_collective",
    "nombre_total_de_partenaire_actif_deux_volets": "total_active_partners_dual_part",
    "nombre_total_de_partenaire_actif_individuel_ou_collectif": "total_active_partners_global",
    "nombre_total_cumule_de_partenaires_actives_individuel": "total_cumulative_activated_partners_individual",
    "nombre_total_cumule_de_partenaires_actives_collectif": "total_cumulative_activated_partners_collective",
    "nombre_total_cumule_de_partenaires_actives_deux_volets": "total_cumulative_activated_partners_dual_part",
    "nombre_total_cumule_de_partenaires_actives_indiv_sans_collectif": "total_cumulative_activated_partners_individual_only",
    "nombre_total_cumule_de_partenaires_actives_collectif_sans_indiv": "total_cumulative_activated_partners_collective_only",
} %}
{% set type_kpi_names = [] %}
{% for partner_type in partner_types %}
    {% do type_kpi_names.append(
        "nombre_de_partenaire_actif_individuel_" ~ partner_type.name
    ) %}
{% endfor %}
{% set geo_columns = [
    "partition_month",
    "partner_region_name",
    "partner_department_name",
    "partner_epci_code",
    "partner_municipality_code",
] %}

with
    -- one row per metrics row: all partners from metrics_cultural_partner__activation,
    -- active individual partners by type from
    -- metrics_cultural_partner__activation_by_type (pivoted, 0 elsewhere)
    partner_cells as (
        select
            {{ geo_columns | join(", ") }},
            {% for kpi_name, column in kpis.items() %}
                {{ column }} as {{ kpi_name }},
            {% endfor %}
            {% for kpi_name in type_kpi_names %}
                0 as {{ kpi_name }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ ref("metrics_cultural_partner__activation") }}
        {% if is_incremental() %}
            where
                partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
        union all
        select
            {{ geo_columns | join(", ") }},
            {% for kpi_name in kpis.keys() %} 0 as {{ kpi_name }}, {% endfor %}
            {% for partner_type in partner_types %}
                if(
                    partner_type = '{{ partner_type.name }}',
                    total_active_partners_individual,
                    0
                ) as {{ type_kpi_names[loop.index0] }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ ref("metrics_cultural_partner__activation_by_type") }}
        {% if is_incremental() %}
            where
                partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    ),

    -- one aggregation per geographic level with every KPI as a column, unpivoted
    -- below (kpi = numerator, denominator = 1)
    final_output as (
        {% for dim in dimensions_geo %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                {% for kpi_name in kpis.keys() | list + type_kpi_names %}
                    sum({{ kpi_name }}) as {{ kpi_name }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from partner_cells
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    )

select
    partition_month,
    timestamp("{{ ts() }}") as updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    1 as denominator,
    numerator as kpi
from
    final_output unpivot (
        numerator for kpi_name
        in ({{ (kpis.keys() | list + type_kpi_names) | join(", ") }})
    )
where dimension_value is not null
