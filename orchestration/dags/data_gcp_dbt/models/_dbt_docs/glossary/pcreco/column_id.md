!!! note
    This file is auto-generated

    :construction_worker_tone1: Work in progress :construction_worker_tone1:

{% docs column__reco_call_id %}The identifier for the recommendation call.{% enddocs %}
{% docs column__cloud_run_revision_name %}The Cloud Run revision name of the recommendation service that served the call.{% enddocs %}
{% docs column__playlist_origin %}The origin type of the playlist. Possible values: `recommendation`, `similar_offer`.{% enddocs %}
{% docs column__reco_context %}The context in which the recommendation was served, combining the playlist origin and the algorithm used. Possible values: `recommendation:user_based`, `recommendation:tops`, `recommendation_fallback:user_based`, `recommendation_fallback:tops`, `similar_offer:user_based`, `similar_offer:tops`, `similar_offer:graph`.{% enddocs %}
