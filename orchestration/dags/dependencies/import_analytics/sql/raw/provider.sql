SELECT
    "isActive" as is_active
    , CAST("id" AS varchar(255)) as provider_id
    , "name" as provider_name
    , "localClass" as local_class
    , "enabledForPro" as enabled_for_pro
FROM public.provider