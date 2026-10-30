The `metrics_beneficiary__weekly_activation` model provides aggregated key indicators related to pass Culture beneficiary onboarding and early activation, computed on a weekly basis.

It consolidates data from weekly onboarding tracking (started onboarding vs. validated within 7 days) and activation tracking (registered beneficiaries vs. first active reservation within 7 days) grouped by signup week and beneficiary age at signup.

## Table Description

Each row represents weekly aggregated onboarding, validation, registration, and 7-day activation metrics for a specific signup week and beneficiary age at signup. We exclude users that had ineligble reasons: age not valid, duplicate ID or INE.

| name                             | data_type | description                                                                                                                                          |
| -------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| signup_week                      |           | The start date of the week during which the beneficiary initiated their registration or onboarding process (format: YYYY-MM-DD, starting on Monday). |
| age_at_signup                    |           | The age reached by the beneficiary at the time of signup/onboarding initiation.                                                                      |
| total_onboarding_users           |           | Total number of users who initiated the onboarding process during the signup week.                                                                   |
| total_validated_7d_users         |           | Total number of users who successfully completed and validated their identity/onboarding within 7 days of initiating it.                             |
| total_registered_beneficiaries   |           | Total number of fully registered beneficiaries whose accounts became active during the signup week.                                                  |
| total_activated_7d_beneficiaries |           | Total number of registered beneficiaries who received their deposit within 7 days following their registration.                                      |
| pct_activated_7d_beneficiaries   |           | Percentage of registered beneficiaries who were activated within 7 days (`total_activated_7d_beneficiaries / total_registered_beneficiaries`).       |
| pct_validated_7d_users           |           | Percentage of onboarding users who validated their onboarding process within 7 days (`total_validated_7d_users / total_onboarding_users`).           |
