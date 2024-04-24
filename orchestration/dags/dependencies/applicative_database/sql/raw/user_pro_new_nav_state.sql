SELECT 
    CAST("id" AS varchar(255)) as user_eligibility_id,
    CAST("userId" AS varchar(255)) as user_id,
    "eligibilityDate" as eligibility_timestamp,
    "newNavDate" as new_nav_timestamp 
FROM public.user_pro_new_nav_state