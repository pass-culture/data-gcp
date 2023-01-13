SELECT
    CAST("id" AS varchar(255)) AS user_id,
    CAST("email" AS VARCHAR(255)) as email
FROM public.user
WHERE "email" like '%@passculture.app' and "isActive"