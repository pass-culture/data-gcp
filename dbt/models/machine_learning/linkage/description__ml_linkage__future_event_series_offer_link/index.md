# Table: Future Event Series Offer Link

The `ml_linkage__future_event_series_offer_link` table contains a preview of the event series/offer link once ingestion of the delta event series offer link and delta event series is completed.

This table is used to validate that the future state of the event series and event offer links are correct before synchronizing with the backend application by running dbt tests on it.

## Table description

| name            | data_type | description                            |
| --------------- | --------- | -------------------------------------- |
| event_series_id |           | Unique identifier of the event series. |
| offer_id        |           | Unique identifier for the offer.       |
