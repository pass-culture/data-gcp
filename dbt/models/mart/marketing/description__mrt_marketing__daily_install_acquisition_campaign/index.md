The `mrt_marketing__daily_install_acquisition_campaign` table provides a consolidated view of daily advertising costs and user attribution metrics. It aggregates key campaign performance indicators, including advertising expenditures, app installations, user registrations, and beneficiary counts across different age groups. By selecting the most recent data per app_install_date, the model enables analysis of user acquisition performance and campaign effectiveness across media sources, campaigns, and individual advertisements. Reporting on campaign, user attribution, and install cost is supported by the model for a defined 14-day attribution window. Key logic includes combining cost and install data from the int_appsflyer\_\_daily_install_cost model with user event data from the int_appsflyer\_\_daily_install_attribution model, aggregating user attribution metrics by campaign attributes and ensuring accurate reporting by selecting only the most recent execution for each app_install_date.

## Table description

| name                         | data_type | description                                                                                     |
| ---------------------------- | --------- | ----------------------------------------------------------------------------------------------- |
| app_install_date             | DATE      | The installation date of the app.                                                               |
| app_os                       | STRING    | The operating system of the user's device. Possible values: 'android', 'ios'.                   |
| acquisition_media_source     | STRING    | The entity responsible for displaying advertisements, such as Facebook or Google Ads..          |
| acquisition_campaign         | STRING    | The name of the advertising campaign.                                                           |
| acquisition_adset            | STRING    | The name of the set of advertisements within a campaign.                                        |
| acquisition_ad               | STRING    | The specific title or identifier of an individual advertisement within a campaign.              |
| total_costs                  | INT64     | The total cost of the advertising campaign tracked by AppsFlyer.                                |
| total_installs               | INT64     | The total number of app installations recorded by AppsFlyer.                                    |
| total_registrations          | INT64     | The total number of registrations recorded by AppsFlyer.                                        |
| total_beneficiaries          | INT64     | The total number of beneficiaries tracked by AppsFlyer.                                         |
| total_beneficiaries_18       | INT64     | The total number of 18-year-old beneficiaries tracked by AppsFlyer.                             |
| total_beneficiaries_underage | INT64     | The total number of underage beneficiaries, categorized by age groups, as tracked by AppsFlyer. |
| total_beneficiaries_17       | INT64     | The total number of 17-year-old beneficiaries tracked by AppsFlyer.                             |
| total_beneficiaries_16       | INT64     | The total number of 16-year-old beneficiaries tracked by AppsFlyer.                             |
| total_beneficiaries_15       | INT64     | The total number of 15-year-old beneficiaries tracked by AppsFlyer.                             |
| total_registrations_15       | INT64     | The total number of 15-year-old registrations tracked by AppsFlyer.                             |
| total_registrations_16       | INT64     | The total number of 16-year-old registrations tracked by AppsFlyer.                             |
| total_registrations_17       | INT64     | The total number of 17-year-old registrations tracked by AppsFlyer.                             |
| total_registrations_18       | INT64     | The total number of 18-year-old registrations tracked by AppsFlyer.                             |
| total_registrations_19_plus  | INT64     | The total number of registrations for users 19 years and older tracked by AppsFlyer.            |
