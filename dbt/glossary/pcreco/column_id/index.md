Note

This file is auto-generated

Work in progress

**reco_call_id**: The identifier for the recommendation call.

**cloud_run_revision_name**: The Cloud Run revision name of the recommendation service that served the call.

**playlist_origin**: The origin playlist endpoint that served the offers. Possible values: `recommendation` (from the recommendation endpoint), `similar_offer` (from the similar offer endpoint, including fallback cases).

**reco_context**: The full context identifier of the recommendation call, combining the algorithm pipeline and strategy. Possible values: `recommendation:user_based`, `recommendation:tops`, `recommendation_fallback:user_based`, `recommendation_fallback:tops`, `similar_offer:user_based`, `similar_offer:tops`, `similar_offer:graph`. Note: `recommendation_fallback` is a fallback mechanism triggered when the `similar_offer` pipeline returns no results — it falls back to the recommendation endpoint and maps to `playlist_origin = 'similar_offer'`.
