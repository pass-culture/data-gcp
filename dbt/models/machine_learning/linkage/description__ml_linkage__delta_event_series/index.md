# Table: Delta Event Series

The `ml_linkage__delta_event_series` table contains the new event series data that must be synchronized with the backend application. It is an export from the ml_preproc\_\_delta_event_series source computed by the event_linkage DAG.

## Table description

| name                        | data_type | description                                                                                        |
| --------------------------- | --------- | -------------------------------------------------------------------------------------------------- |
| event_series_id             |           | Unique identifier of the event series.                                                             |
| event_series_name           |           | Name of the event series.                                                                          |
| event_series_description    |           | Short description of the event series.                                                             |
| event_series_image_url      |           | URL of the image associated with the event series.                                                 |
| event_series_mediation_uuid |           | Name of the image file related to the event series. Is a deterministic UUID based on the file URL. |
| action                      |           | Action to be taken by the backend during synchronization.(can be null if not matched).             |
| comment                     |           | Comment or additional information related to the delta action.(can be null if not matched).        |
