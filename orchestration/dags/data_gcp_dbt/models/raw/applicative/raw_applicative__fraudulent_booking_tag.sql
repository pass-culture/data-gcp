select *
from
    external_query(
        "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
        """
SELECT
    CAST("id" AS varchar(255)) AS fraudulent_booking_tag_id,
    "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS fraudulent_booking_tag_created_at,
    CAST("authorId" AS varchar(255)) AS fraudulent_booking_tag_author_id,
    CAST("bookingId" AS varchar(255)) AS booking_id
    FROM public.fraudulent_booking_tag
"""
    )
