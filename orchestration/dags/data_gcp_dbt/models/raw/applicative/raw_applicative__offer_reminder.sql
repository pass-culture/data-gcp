select * from external_query("{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}", """
SELECT
    CAST("id" AS varchar(255)) AS offer_reminder_id,
    CAST("userId" AS varchar(255)) AS user_id,
    CAST("offerId" AS varchar(255)) AS offer_id
    FROM public.offer_reminder
""")
