SELECT 
    qe.id as execution_id,
    qe.started_at as execution_date,
    qe.hash,
    qe.running_time,
    qe.result_rows,
    qe.context,
    qe.error,
    qe.cache_hit,
    qe.executor_id,
    qe.dashboard_id,
    qe.card_id
FROM public.query_execution qe
WHERE card_id is not null