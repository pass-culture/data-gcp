{% snapshot snapshot_raw__booking %}

    {{
        config(
            **custom_snapshot_config(
                strategy="check",
                unique_key="booking_id",
                check_cols=[
                    "booking_creation_date",
                    "stock_id",
                    "booking_quantity",
                    "user_id",
                    "booking_amount",
                    "booking_status",
                    "booking_is_cancelled",
                    "booking_is_used",
                    "booking_used_date",
                    "booking_cancellation_date",
                    "booking_cancellation_reason",
                    "booking_reimbursement_date",
                ],
            )
        )
    }}

    select
        booking_id,
        booking_creation_date,
        stock_id,
        booking_quantity,
        user_id,
        booking_amount,
        booking_status,
        booking_is_cancelled,
        booking_is_used,
        booking_used_date,
        booking_cancellation_date,
        booking_cancellation_reason,
        booking_reimbursement_date,
        deposit_id,
        offerer_id,
        venue_id,
        price_category_label,
        booking_used_recredit_type
    from
        external_query(
            "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
            """SELECT
        CAST("id" AS varchar(255)) as booking_id
        , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as booking_creation_date
        , CAST("stockId" AS varchar(255)) as stock_id
        , "quantity" as booking_quantity
        , CAST("userId" AS varchar(255)) as user_id
        , "amount" as booking_amount
        , CAST("status" AS varchar(255)) AS booking_status
        , "status" = \'CANCELLED\' AS booking_is_cancelled
        , "status" IN (\'USED\', \'REIMBURSED\') as booking_is_used
        , "status" = \'REIMBURSED\' AS reimbursed
        , "dateUsed" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as booking_used_date
        , "cancellationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as booking_cancellation_date
        , CAST("cancellationReason" AS VARCHAR) AS booking_cancellation_reason
        , CAST("depositId" AS varchar(255)) as deposit_id
        , CAST("offererId" AS varchar(255)) as offerer_id
        , CAST("venueId" AS varchar(255)) as venue_id
        ,"priceCategoryLabel" AS price_category_label
        , "reimbursementDate" AS booking_reimbursement_date
        , "usedRecreditType" AS booking_used_recredit_type
    FROM public.booking
    """
        )

{% endsnapshot %}
