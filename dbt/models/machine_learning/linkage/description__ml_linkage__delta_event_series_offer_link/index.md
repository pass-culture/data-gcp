# Table: Delta Event Series Offer Link

The `ml_linkage__delta_event_series_offer_link` table contains the new event series/offer link data that must be synchronized with the backend application. It is an export from the ml_preproc\_\_delta_event_series_offer_link source computed by the event_linkage DAG.

## Table description

| name            | data_type | description                                                                                 |
| --------------- | --------- | ------------------------------------------------------------------------------------------- |
| event_series_id |           | Unique identifier of the event series.                                                      |
| offer_id        |           | Unique identifier for the offer.                                                            |
| action          |           | Action to be taken by the backend during synchronization.(can be null if not matched).      |
| comment         |           | Comment or additional information related to the delta action.(can be null if not matched). |
