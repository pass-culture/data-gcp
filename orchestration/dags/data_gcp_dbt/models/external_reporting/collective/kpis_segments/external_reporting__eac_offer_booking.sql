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
    Roll-up of the collective offer and booking metrics to the reporting
    dimensions. All KPIs are monthly flows. KPIs without suffix follow the
    venue (cultural partner) geography, KPIs suffixed `_etablissements`
    follow the school geography.
#}
{% set sources = [
    {
        "model": "metrics_offer__collective",
        "side": "venue",
        "kpis": {
            "total_offres_collectives_creees": "total_created_collective_offers",
        },
    },
    {
        "model": "metrics_offer__collective_bookable",
        "side": "venue",
        "kpis": {
            "total_offres_reservables_creees": "total_created_bookable_collective_offers",
            "total_offres_reservables_prereservees": "total_prereserved_bookable_collective_offers",
            "total_partenaires_offres_reservables": "new_bookable_offer_partners",
        },
    },
    {
        "model": "metrics_offer__collective_bookable_institution",
        "side": "institution",
        "kpis": {
            "total_offres_reservables_recues_etablissements": "total_created_bookable_collective_offers",
        },
    },
    {
        "model": "metrics_booking__collective",
        "side": "venue",
        "kpis": {
            "total_reservations_collectives": "total_collective_bookings",
            "total_montant_engage": "total_collective_amount_spent",
            "total_prereservations_collectives": "total_prereservations",
            "total_confirmations_collectives": "total_confirmations",
            "total_actions_collectives": "total_actions",
            "total_montant_actions": "total_action_amount",
        },
    },
    {
        "model": "metrics_booking__collective_institution",
        "side": "institution",
        "kpis": {
            "total_reservations_collectives_etablissements": "total_collective_bookings",
            "total_montant_engage_etablissements": "total_collective_amount_spent",
            "total_prereservations_collectives_etablissements": "total_prereservations",
            "total_confirmations_collectives_etablissements": "total_confirmations",
            "total_actions_collectives_etablissements": "total_actions",
            "total_montant_actions_etablissements": "total_action_amount",
        },
    },
] %}

{% for source in sources %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        partition_month,
        timestamp('{{ ts() }}') as updated_at,
        dimension_name,
        dimension_value,
        kpi_name,
        numerator,
        1 as denominator,
        numerator as kpi
    from
        (
            {% for dim in get_dimensions(source.side, "academic_extended") %}
                {% if not loop.first %}
                    union all
                {% endif %}
                select
                    partition_month,
                    '{{ dim.name }}' as dimension_name,
                    {{ dim.value_expr }} as dimension_value,
                    {% for kpi_name, column in source.kpis.items() %}
                        cast(sum({{ column }}) as float64) as {{ kpi_name }}
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                from {{ ref(source.model) }}
                where
                    1 = 1
                    {% if is_incremental() %}
                        and partition_month = date_trunc(
                            date_sub(date('{{ ds() }}'), interval 1 month), month
                        )
                    {% endif %}
                group by partition_month, dimension_name, dimension_value
            {% endfor %}
        )
        unpivot (numerator for kpi_name in ({{ source.kpis.keys() | join(", ") }}))
{% endfor %}
