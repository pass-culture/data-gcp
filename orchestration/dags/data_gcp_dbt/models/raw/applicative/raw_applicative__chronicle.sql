select *
from
    external_query(
        "{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}",
        """
SELECT
    CAST("id" AS varchar(255)) AS chronicle_id,
    "dateCreated" AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Paris' AS chronicle_created_at,
    CAST("userId" AS varchar(255)) AS user_id,
    "isActive" AS chronicle_is_active,
    "content" AS chronicle_content,
    "clubType" AS chronicle_club_type
FROM public.chronicle
"""
    )
