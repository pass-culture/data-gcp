select
    special_event_response_id,
    special_event_id,
    user_id,
    special_event_response_status,
    special_event_response_submitted_date
from
    external_query(
        "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
        """
SELECT
    CAST("id" AS varchar(255)) AS special_event_response_id,
    CAST("eventId" AS varchar(255)) AS special_event_id,
    CAST("userId" AS varchar(255)) AS user_id,
    CAST("status" AS varchar(255)) AS special_event_response_status,
    "dateSubmitted" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS special_event_response_submitted_date
    FROM public.special_event_response
"""
    )
