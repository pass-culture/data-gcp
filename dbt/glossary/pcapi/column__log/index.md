Note

This file is auto-generated

Work in progress

*Base columns for log entries coming from pcapi backend logging.*

**log_timestamp**: The timestamp when the log entry was recorded.

**partition_date**: The date used for partitioning the log data.

**environement**: The environment in which the log entry was recorded, such as production or staging.

**message**: The message content of the log entry, describing the event or action.

**technical_message_id**: The technical identifier for the message associated with the log entry.

**device_id**: The identifier for the device used during the log entry event.

**analytics_source**: The source of analytics data associated with the log entry. The mart keeps the log entries of the pro application plus a set of log lines selected by their message; the latter can have a NULL source.

**choice_datetime**: The timestamp when the cookie conset was recorded.
