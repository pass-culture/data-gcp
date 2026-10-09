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
        booking_ref=ref("mrt_global__collective_booking"),
        date_column="collective_booking_used_date",
        amount_column="booking_amount",
    )
}}
