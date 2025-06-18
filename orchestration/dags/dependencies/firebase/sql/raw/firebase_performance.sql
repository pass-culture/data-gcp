select *,  TIMESTAMP_TRUNC(event_timestamp, DAY) as event_date
from `{{ params.gcp_project_env }}.app_passculture_{{ params.suffix }}`
WHERE TIMESTAMP_TRUNC(event_timestamp, DAY) = TIMESTAMP("{{ ds }}")
