# Table: Backend Feature Logs

This model extracts backend API logs tagged with a `technical_origin` (origin / feature / action), logged by the backend since 2026-09-17, to monitor the error rate of backend features.

Each row is one log entry. An action is considered successful when the log level is 'INFO' (or when the explicit `success` flag is true for logs carrying it, e.g. `users.login`), and failed when logged as 'WARNING' or 'ERROR'.

The full `extra` payload is kept in `extra_json`, so fields added later by the backend can be used without changing the model.

## Table description

| name              | data_type | description                                                                                                                                                                                                |
| ----------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| partition_date    |           | The date used for partitioning the log data.                                                                                                                                                               |
| log_timestamp     |           | The timestamp when the log entry was recorded.                                                                                                                                                             |
| origin            |           | Application emitting the log, from `technical_origin.origin`: 'native' (young users app), 'pro' (pro portal), 'provider' (public API used by providers), 'backoffice', 'cron' (scheduled jobs), 'discord'. |
| feature           |           | Backend feature the log relates to, from `technical_origin.feature` (e.g. 'bookings', 'external_providers', 'users').                                                                                      |
| action            |           | Action performed on the feature, from `technical_origin.action` (e.g. 'book', 'cancel', 'login').                                                                                                          |
| cron_name         |           | Name of the scheduled job emitting the log, when origin is 'cron'.                                                                                                                                         |
| severity          |           | Log level of the backend log entry:                                                                                                                                                                        |
|                   |           | - 'INFO': the action succeeded.                                                                                                                                                                            |
|                   |           | - 'WARNING': the action failed in an expected way (e.g. business rule refusal). Today all handled failures are logged as 'WARNING', provider API errors included.                                          |
|                   |           | - 'ERROR': the action failed in an unexpected way.                                                                                                                                                         |
| is_success        |           | True when the action succeeded. Uses the explicit `success` flag when the log has one (e.g. users.login), otherwise the log level ('INFO' = success, 'WARNING' / 'ERROR' = failure).                       |
| message           |           | The message content of the log entry, describing the event or action.                                                                                                                                      |
| exception_class   |           | Python exception class raised by the backend when the action failed (e.g. 'OfferIsAlreadyBooked', 'BoostAPIException'). NULL on success.                                                                   |
| exception_message |           | Message of the exception raised by the backend, often empty for business rule refusals and detailed for provider API errors. NULL on success.                                                              |
| provider_id       |           | The unique identifier for the synchronization provider.                                                                                                                                                    |
| user_id           |           | Unique identifier for a user.                                                                                                                                                                              |
| booking_id        |           | Unique identifier for a booking.                                                                                                                                                                           |
| offer_id          |           | Unique identifier for the offer.                                                                                                                                                                           |
| stock_id          |           | Unique identifier for the stock.                                                                                                                                                                           |
| venue_id          |           | Unique identifier for the venue.                                                                                                                                                                           |
| trace             |           | Cloud Logging trace identifier, used to link log entries emitted by the same backend request.                                                                                                              |
| extra_json        |           | Full `extra` payload of the backend log entry as JSON (null and empty fields removed), to access fields not yet extracted as columns, e.g. `json_value(extra_json.stock_id_at_providers)`.                 |
