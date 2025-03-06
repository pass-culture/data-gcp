select * from external_query("{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}", """
SELECT
    CAST("id" AS varchar(255)) AS offer_chronicle_id,
    CAST("offerId" AS varchar(255)) AS offer_id,
    CAST("chronicleId" AS varchar(255)) AS chronicle_id,
    FROM public.offer_chronicle
""")
