{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions('user', 'geo') %}

{% set kpis = [
    {
        "name": "montant_moyen_octroye_a_l_expiration_du_credit",
        "numerator_field": "total_deposit_amount",
    },
    {
        "name": "montant_moyen_depense_a_l_expiration_du_credit",
        "numerator_field": "total_actual_amount_spent",
    },
] %}

{{ generate_deposit_usage_metrics(kpis, dimensions) }}
