{% snapshot snapshot_raw__collective_booking %}

    {{
        config(
            **custom_snapshot_config(
                strategy="check",
                unique_key="collective_booking_id",
                check_cols=[
                    "collective_booking_creation_date",
                    "collective_booking_used_date",
                    "collective_stock_id",
                    "venue_id",
                    "offerer_id",
                    "collective_booking_cancellation_date",
                    "collective_booking_cancellation_limit_date",
                    "collective_booking_cancellation_reason",
                    "collective_booking_status",
                    "collective_booking_reimbursement_date",
                    "educational_institution_id",
                    "educational_year_id",
                    "collective_booking_confirmation_date",
                    "collective_booking_confirmation_limit_date",
                    "educational_redactor_id",
                ],
            )
        )
    }}

    select
        collective_booking_id,
        collective_booking_creation_date,
        collective_booking_used_date,
        collective_stock_id,
        venue_id,
        offerer_id,
        collective_booking_cancellation_date,
        collective_booking_cancellation_limit_date,
        collective_booking_cancellation_reason,
        collective_booking_status,
        collective_booking_reimbursement_date,
        educational_institution_id,
        educational_year_id,
        collective_booking_confirmation_date,
        collective_booking_confirmation_limit_date,
        educational_redactor_id
    from
        external_query(
            "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
            '''SELECT
        CAST("id" AS varchar(255)) AS collective_booking_id
        , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_booking_creation_date
        , "dateUsed" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_booking_used_date
        , CAST("collectiveStockId" AS varchar(255)) AS collective_stock_id
        , CAST("venueId" AS varchar(255)) AS venue_id
        , CAST("offererId" AS varchar(255)) AS offerer_id
        , "cancellationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_booking_cancellation_date
        , "cancellationLimitDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_booking_cancellation_limit_date
        , CAST("cancellationReason" AS VARCHAR) AS collective_booking_cancellation_reason
        , CAST("status" AS VARCHAR) AS collective_booking_status
        , "reimbursementDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_booking_reimbursement_date
        , CAST("educationalInstitutionId" AS varchar(255)) AS educational_institution_id
        , CAST("educationalYearId" AS varchar(255)) AS educational_year_id
        , "confirmationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_booking_confirmation_date
        , "confirmationLimitDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_booking_confirmation_limit_date
        , CAST("educationalRedactorId" AS varchar(255)) AS educational_redactor_id
    FROM public.collective_booking
    '''
        )

{% endsnapshot %}
