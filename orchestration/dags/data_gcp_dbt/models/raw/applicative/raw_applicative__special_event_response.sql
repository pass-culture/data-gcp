select * from external_query("{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}", """
SELECT
    CAST("id" AS varchar(255)) AS event_response_id,
    CAST("eventId" AS varchar(255)) AS event_id,
    CAST("userId" AS varchar(255)) AS user_id,
    CAST("status" AS varchar(255)) AS response_status,
    "dateSubmitted" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS response_submitted_date
    FROM public.special_event_response
""")
