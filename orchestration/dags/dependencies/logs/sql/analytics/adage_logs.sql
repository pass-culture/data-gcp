SELECT
    DATE(timestamp) as partition_date,
    timestamp,
    jsonPayload.extra.path AS path,
    jsonPayload.extra.statuscode AS status_code,
    jsonPayload.extra.method AS method,
    jsonPayload.extra.sourceip AS source_ip,
    jsonPayload.extra.duration AS duration
FROM
    `{{ bigquery_raw_dataset }}.stdout`
WHERE
    DATE(timestamp) = "{{ ds }}" AND jsonPayload.extra.path LIKE "%adage-iframe%"