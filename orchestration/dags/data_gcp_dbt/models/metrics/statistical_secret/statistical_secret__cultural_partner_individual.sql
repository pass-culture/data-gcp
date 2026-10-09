{{
    config(
        materialized="table",
        partition_by={
            "field": "partition_month",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["department_code", "epci_code", "municipality_code"],
    )
}}

{{
    evaluate_statistical_secret(
        booking_ref=ref("int_global__booking"),
        date_column="booking_used_date",
        amount_column="booking_intermediary_amount",
    )
}}
