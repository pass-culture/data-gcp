select
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
from public.query_execution qe
where card_id is not null
