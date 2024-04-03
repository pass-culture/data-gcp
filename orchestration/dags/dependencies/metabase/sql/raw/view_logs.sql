SELECT 
    qe.id,
    qe.user_id,
    qe.timestamp,
    qe.model,
    qe.model_id,
    qe.metadata
FROM public.view_logs qe