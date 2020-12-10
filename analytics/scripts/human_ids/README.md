# Human id functions

The scripts in this folder are used to transform the ids into human readable ids, and back.

The base ids are used in the database, the humanized ids are used in the frontend.

The original functions can be found here : `pass-culture-api/src/pcapi/utils/human_ids.py`

The file **base32.js** is required to use the functions, it takes place in GCS.


## Todo
The function humanize is also defined and used in `orchestration/dags/dependencies/data_analytics/enriched_data/humanize_id.js`, we should keep only one script.
