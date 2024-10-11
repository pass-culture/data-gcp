select
    query_hash,
    average_execution_time,
    query::json -> 'native' -> 'query' as query,
    query as query_raw
from public.query
