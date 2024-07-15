SELECT 
    id,
    model,
    model_id,
    user_id,
    timestamp,
    object,
    is_reversion,
    is_creation,
    message,
    most_recent,
    metabase_version
FROM public.revision