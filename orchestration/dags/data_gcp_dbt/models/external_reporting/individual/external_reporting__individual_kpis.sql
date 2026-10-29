{{ config(materialized="table") }}

{#
    Union of the KPI segments read by external_reporting__individual. Materialized as a
    table so that the statistical secret logic, which reads it several times, does
    not re-plan every segment (the segments are views in CI).
    A territory is secret if it is secret on the individual or the collective side
    (cultural partner pages show both).

    For each segment:
        - is_secret_applicable selects the KPIs subject to the statistical secret:
          the ones measured on the cultural partner (venue) geography. KPIs
          measured on the beneficiary geography are never masked;
        - aggregation tells consumers how to aggregate over months: "flow" = sum
          the months (and a row every month, see apply_statistical_secret),
          "stock" = read a single month (snapshot, cumulative, average or share).
#}
{% set models = {
    "beneficiary": {
        "is_secret_applicable": "false",
        "aggregation": "'stock'",
    },
    "coverage": {"is_secret_applicable": "false", "aggregation": "'stock'"},
    "cultural_partner": {
        "is_secret_applicable": "true",
        "aggregation": "'stock'",
    },
    "collective_partner_booking": {
        "is_secret_applicable": "true",
        "aggregation": "'flow'",
    },
    "beneficiary_booking": {
        "is_secret_applicable": "false",
        "aggregation": "'flow'",
    },
    "deposit_usage": {
        "is_secret_applicable": "false",
        "aggregation": "'stock'",
    },
    "diversity": {"is_secret_applicable": "false", "aggregation": "'stock'"},
    "finance": {"is_secret_applicable": "true", "aggregation": "'flow'"},
    "booking": {
        "is_secret_applicable": "kpi_name not like 'part_partenaires_dans_conso_beneficiaires%'",
        "aggregation": "if(kpi_name in ('total_reservations_gratuites', 'total_reservations_toutes'), 'flow', 'stock')",
    },
} %}
{% for model, segment_config in models.items() %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        partition_month,
        updated_at,
        dimension_name,
        dimension_value,
        kpi_name,
        coalesce(numerator, 0) as numerator,
        coalesce(denominator, 0) as denominator,
        coalesce(kpi, 0) as kpi,
        {{ segment_config.aggregation }} as aggregation,
        {{ segment_config.is_secret_applicable }} as is_secret_applicable
    from {{ ref("external_reporting__" ~ model) }}
    where
        partition_month
        <= date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        and partition_month >= date("2022-01-01")
{% endfor %}
