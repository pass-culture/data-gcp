select id, user_id, booking_id, name, unlocked_date, seen_date
from external_query("{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}", """
SELECT
    CAST("id" AS varchar(255)) AS id,
    CAST("userId" AS varchar(255)) AS user_id,
    CAST("bookingId" AS varchar(255)) AS booking_id,
    CAST("name" AS varchar(255)) AS name,
    "unlockedDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS unlocked_date,
    "seenDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS seen_date
    FROM public.achievement
""")
