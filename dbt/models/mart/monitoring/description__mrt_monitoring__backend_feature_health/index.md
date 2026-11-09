# Table: Backend Feature Health

Daily count of successful and failed backend actions, to monitor the error rate of backend features (e.g. bookings and booking cancellations from the app) in Metabase.

All origin / feature / action logged since 2026-09-17 are kept, so new features are picked up automatically.

Each row is a combination of date, origin, feature, action, severity, exception_class and provider_id.

Counts are actions, not log lines: the lines of one request (trace) about the same booking are one action, which fails when any of its lines fails. A line without booking_id is attached to its request's booking when the request has exactly one; otherwise each line is an action (bulk actions log one line per stock or offer). `log_line_count` keeps the raw line count.

Error rate must be computed from the counts, never averaged: `sum(error_count) / sum(total_count)`.

## Table description

| name            | data_type | description                                                                                                                                                                                                         |
| --------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| partition_date  |           | The date used for partitioning the log data.                                                                                                                                                                        |
| origin          |           | Application emitting the log, from `technical_origin.origin`: 'native' (young users app), 'pro' (pro portal), 'provider' (public API used by providers), 'backoffice', 'cron' (scheduled jobs), 'discord'.          |
| feature         |           | Backend feature the log relates to, from `technical_origin.feature` (e.g. 'bookings', 'external_providers', 'users').                                                                                               |
| action          |           | Action performed on the feature, from `technical_origin.action` (e.g. 'book', 'cancel', 'login').                                                                                                                   |
| severity        |           | Log level of the backend log entry:                                                                                                                                                                                 |
|                 |           | - 'INFO': the action succeeded.                                                                                                                                                                                     |
|                 |           | - 'WARNING': the action failed in an expected way (e.g. business rule refusal). Today all handled failures are logged as 'WARNING', provider API errors included.                                                   |
|                 |           | - 'ERROR': the action failed in an unexpected way.                                                                                                                                                                  |
| exception_class |           | Python exception class raised by the backend when the action failed (e.g. 'OfferIsAlreadyBooked', 'BoostAPIException'). NULL on success.                                                                            |
| provider_id     |           | The unique identifier for the synchronization provider.                                                                                                                                                             |
| total_count     |           | Number of actions for the given dimension combination: one per request (trace) and booking, not one per log line.                                                                                                   |
| success_count   |           | Number of successful actions for the given dimension combination.                                                                                                                                                   |
| error_count     |           | Number of failed actions for the given dimension combination.                                                                                                                                                       |
| log_line_count  |           | Number of log lines behind the actions counted in total_count. Higher than total_count when an action logs several lines (e.g. a booking cancellation logs a start marker, the core outcome and the route outcome). |
