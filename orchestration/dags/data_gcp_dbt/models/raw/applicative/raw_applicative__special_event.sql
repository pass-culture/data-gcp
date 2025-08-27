select
    special_event_id,
    special_event_title,
    offerer_id,
    venue_id,
    typeform_id,
    special_event_creation_date,
    special_event_date
from
    external_query(
        "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
        """
SELECT
    CAST("id" AS varchar(255)) AS special_event_id,
    CAST("title" AS varchar(255)) AS special_event_title,
    CAST("offererId" AS varchar(255)) AS offerer_id,
    CAST("venueId" AS varchar(255)) AS venue_id,
    CAST("externalId" AS varchar(255)) AS typeform_id,
    "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS special_event_creation_date,
    "eventDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS special_event_date
    FROM public.special_event
"""
    )
