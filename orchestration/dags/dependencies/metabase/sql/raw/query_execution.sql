SELECT 
    qe.id,
    qe.started_at,
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
