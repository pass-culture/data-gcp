---
title: Identity check columns
description: Column documentation for identity checks (`beneficiary_fraud_check` rows of an identity provider).
---

{% docs column__identity_check_id %} Unique identifier of the identity check (the `beneficiary_fraud_check` row in the backend). {% enddocs %}
{% docs column__identity_check_type %} Identity provider of the check: UBBLE (remote check with an ID document and a video), DMS (application filed on Démarches Simplifiées and processed by an operator), EDUCONNECT (school account, for minors), JOUVE (provider used until early 2022). A user can have several checks, of several types. {% enddocs %}
{% docs column__identity_check_status %} Current status of the check: STARTED or PENDING (in progress), OK (identity validated), KO (refused), SUSPICIOUS (needs a manual review), CANCELED (abandoned), ERROR (technical error). NULL on a few old checks. The status is the latest one: earlier statuses are not kept. {% enddocs %}
{% docs column__identity_check_eligibility_type %} Credit the user was applying for when the check was made: AGE18 (18-year-old credit before the March 2025 reform), UNDERAGE (15-17 credit before the reform), AGE17_18 (17-18 credit since the reform), FREE (15-16 free-offer profile since the reform). NULL when the user was not eligible. {% enddocs %}
{% docs column__identity_check_reason_code %} First reason code recorded on the check (e.g. a refusal or suspicion reason such as id_check_expired, duplicate_user, age_too_old); NULL when none. Only the first code of the backend list is imported. {% enddocs %}
{% docs column__identity_check_creation_date %} Date and time (UTC) the check was created in the backend, when the user's attempt with this provider started. {% enddocs %}
{% docs column__identity_check_update_date %} Date and time (UTC) of the last change to the check row. For a check in a final status (OK, KO, CANCELED), it is normally when that status was set, so the time from identity_check_creation_date to it measures the check's processing delay; any later edit of the row moves it. NULL when the backend never set it. {% enddocs %}
