select * from external_query("{{ env_var('APPLICATIVE_EXTERNAL_CONNECTION_ID') }}", """
SELECT
    CAST("id" AS varchar(255)) AS chronicle_id,
    CAST("userId" AS varchar(255)) AS user_id,
    CAST("isActive" AS varchar(255)) AS user_is_active,
    CAST("city" AS varchar(255)) AS user_city,
    CAST("age" AS varchar(255)) AS user_age,
    CAST("isIdentityDiffusible" AS varchar(255)) AS user_is_identity_diffusible,
    CAST("isSocialMediaDiffusible" AS varchar(255)) AS user_is_social_media_diffusible,
    CAST("content" AS varchar(255)) AS chronicle_content,
    "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS chronicle_creation_date,
    CAST("ean" AS varchar(255)) AS ean,
    FROM public.chronicle
""")
