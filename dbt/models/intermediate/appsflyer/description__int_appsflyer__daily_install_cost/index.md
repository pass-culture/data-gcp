The `int_appsflyer__daily_install_cost` table summarizes daily advertising costs and app installations from the AppsFlyer platform, selecting the most recent execution date for each app_install_date. By consolidating data across media sources, campaigns, ad sets, and individual ads, it provides insights into campaign expenditures.

## Table description

| name                       | data_type | description                                                                            |
| -------------------------- | --------- | -------------------------------------------------------------------------------------- |
| app_os                     | STRING    | The operating system of the user's device. Possible values: 'android', 'ios'.          |
| acquisition_media_source   | STRING    | The entity responsible for displaying advertisements, such as Facebook or Google Ads.. |
| acquisition_campaign       | STRING    | The name of the advertising campaign.                                                  |
| acquisition_adset          | STRING    | The name of the set of advertisements within a campaign.                               |
| acquisition_ad             | STRING    | The specific title or identifier of an individual advertisement within a campaign.     |
| app_install_date           | DATE      | The installation date of the app.                                                      |
| acquisition_execution_date | DATE      | The execution date for retroactive acquisition cost updates.                           |
| total_costs                | INT64     | The total cost of the advertising campaign tracked by AppsFlyer.                       |
| total_installs             | INT64     | The total number of app installations recorded by AppsFlyer.                           |
