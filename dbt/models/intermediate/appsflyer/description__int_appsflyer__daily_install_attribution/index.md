The `int_appsflyer__daily_install_attribution` table aggregates user event data from the AppsFlyer platform, selecting the most recent version for each combination of app_install_date and acquisition_days_post_attribution. It consolidates data on app installations, user registrations, and beneficiary metrics, offering a comprehensive view of user acquisition performance. By capturing advertising campaign details such as media sources, campaign names, and ad-level breakdowns, this model provides insights for evaluating user engagement across different acquisition channels. The defined attribution window allows for focused analysis of early user behaviors and campaign effectiveness within a 14-day post-installation period.

## Table description

| name                              | data_type | description                                                                                     |
| --------------------------------- | --------- | ----------------------------------------------------------------------------------------------- |
| app_os                            | STRING    | The operating system of the user's device. Possible values: 'android', 'ios'.                   |
| acquisition_media_source          | STRING    | The entity responsible for displaying advertisements, such as Facebook or Google Ads..          |
| acquisition_campaign              | STRING    | The name of the advertising campaign.                                                           |
| acquisition_adset                 | STRING    | The name of the set of advertisements within a campaign.                                        |
| acquisition_ad                    | STRING    | The specific title or identifier of an individual advertisement within a campaign.              |
| acquisition_version               | STRING    | The subsequent version of a report containing all available data for a specific day.            |
| acquisition_days_post_attribution | INT64     | The number of days that have elapsed since the app's installation date.                         |
| app_install_date                  | DATE      | The installation date of the app.                                                               |
| total_registrations               | INT64     | The total number of registrations recorded by AppsFlyer.                                        |
| total_beneficiaries               | INT64     | The total number of beneficiaries tracked by AppsFlyer.                                         |
| total_beneficiaries_18            | INT64     | The total number of 18-year-old beneficiaries tracked by AppsFlyer.                             |
| total_beneficiaries_underage      | INT64     | The total number of underage beneficiaries, categorized by age groups, as tracked by AppsFlyer. |
| total_beneficiaries_17            | INT64     | The total number of 17-year-old beneficiaries tracked by AppsFlyer.                             |
| total_beneficiaries_16            | INT64     | The total number of 16-year-old beneficiaries tracked by AppsFlyer.                             |
| total_beneficiaries_15            | INT64     | The total number of 15-year-old beneficiaries tracked by AppsFlyer.                             |
| total_registrations_15            | INT64     | The total number of 15-year-old registrations tracked by AppsFlyer.                             |
| total_registrations_16            | INT64     | The total number of 16-year-old registrations tracked by AppsFlyer.                             |
| total_registrations_17            | INT64     | The total number of 17-year-old registrations tracked by AppsFlyer.                             |
| total_registrations_18            | INT64     | The total number of 18-year-old registrations tracked by AppsFlyer.                             |
| total_registrations_19_plus       | INT64     | The total number of registrations for users 19 years and older tracked by AppsFlyer.            |
