select *, timestamp_trunc(event_timestamp, day) as event_date
from `{{ params.gcp_project_env }}.app_passculture_{{ params.suffix }}`
where timestamp_trunc(event_timestamp, day) = timestamp("{{ ds }}")
