select * from external_query("{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}", """
SELECT
    CAST("id" AS varchar(255)) AS product_chronicle_id,
    CAST("productId" AS varchar(255)) AS product_id,
    CAST("chronicleId" AS varchar(255)) AS chronicle_id
FROM public.product_chronicle
""")
