-- Join with firebase_events to get the number of sessions
-- & compute indicators.
-- *** Missing utm 

SELECT *

FROM `{{ bigquery_raw_dataset }}.sendinblue_newsletters`