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
    Collective side of the cultural partners (venues), by partner geography.
    Venue counts are additive across geographic cells (a venue has one
    location), so every level is a sum of the metrics rows.
    - pct_partenaire_culturel_actif_collectif: active collective partners
      (bookable collective offer in the last 365 days) over cumulative activated
      collective partners, on the geographies that have at least one partner
      with a collective offer.
    - total_partenaire_avec_offre_vitrine: cumulative number of partners with at
      least one collective template offer ("offre vitrine") at the end of the
      month, every month from the first template partner of the territory.
#}
{% set dimensions = get_dimensions("partner", "academic_extended") %}
{% set incremental_filter %}
    {% if is_incremental() %}
        where
            partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
{% endset %}

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'pct_partenaire_culturel_actif_collectif' as kpi_name,
        sum(total_active_partners_collective) as numerator,
        sum(total_cumulative_activated_partners_collective) as denominator,
        safe_divide(
            sum(total_active_partners_collective),
            sum(total_cumulative_activated_partners_collective)
        ) as kpi
    from {{ ref("metrics_cultural_partner__activation") }} {{ incremental_filter }}
    group by partition_month, dimension_name, dimension_value
    having sum(total_partners_with_collective_offer) > 0
    union all
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'total_partenaire_avec_offre_vitrine' as kpi_name,
        sum(total_cumulative_partners_with_template_offer) as numerator,
        1 as denominator,
        sum(total_cumulative_partners_with_template_offer) as kpi
    from {{ ref("metrics_cultural_partner__template_offer") }} {{ incremental_filter }}
    group by partition_month, dimension_name, dimension_value
{% endfor %}
