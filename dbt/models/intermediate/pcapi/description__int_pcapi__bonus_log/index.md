# Table: Bonus Application Logs

This model parses, flattens, and normalizes JSON payloads from backend API logs related to youth bonus credit requests (`technical_message_id = 'bonus_credit.statistics.counters'`).

It transforms nested JSON counters into an unpivoted event model, breaking down requests into three main statuses:

- **Grants**: Successful bonus allocations per bonus type.
- **Errors**: Rejections and error reasons per bonus type.
- **Attempts**: Distribution of the number of attempts required by users before obtaining a grant.

Each row represents an aggregated metrics period defined by a start timestamp (`log_started_at`) and an end/publication timestamp (`log_ended_at`).

## Table description

| name           | data_type | description                                                                                                                                                            |
| -------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| partition_date |           | The date used for partitioning the log data.                                                                                                                           |
| log_started_at |           | UTC timestamp marking the beginning of the API counting window (counters_since).                                                                                       |
| log_ended_at   |           | UTC timestamp marking the end and publication time of the API counting window (published_at).                                                                          |
| bonus_type     |           | Type of bonus credit requested (e.g., 'qf_bonus_credit', 'aah_bonus_credit', 'aeeh_bonus_credit'). NULL when status = 'attempt'.                                       |
| status         |           | Metric type of the aggregated row: 'grant' (success), 'error' (failure/rejection), or 'attempt' (friction).                                                            |
| error_reason   |           | Specific error code or reason for rejection (e.g., 'not_in_tax_household', 'person_not_found', 'not_recipient'). Populated only when status = 'error', NULL otherwise. |
| attempts_count |           | Number of attempts required before successfully granting the bonus (e.g., 1, 2, 3, 4, 6). Populated only when status = 'attempt', NULL otherwise.                      |
| total_requests |           | Aggregated count of requests for the given dimension combination over the counting period.                                                                             |
