# Table: Future Event Series

The `ml_linkage__future_event_series` table contains a preview of the event series once ingestion of the delta event series is completed.

This table is used to validate that the future state of the event series is correct before synchronizing with the backend application by running dbt tests on it.

## Table description

| name                        | data_type | description                                                                                        |
| --------------------------- | --------- | -------------------------------------------------------------------------------------------------- |
| event_series_id             |           | Unique identifier of the event series.                                                             |
| event_series_name           |           | Name of the event series.                                                                          |
| event_series_description    |           | Short description of the event series.                                                             |
| event_series_mediation_uuid |           | Name of the image file related to the event series. Is a deterministic UUID based on the file URL. |
