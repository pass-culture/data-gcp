---
title: Daily Install Acquisition Campaign
description: Description of the `mrt_marketing__daily_install_acquisition_campaign` table.
---

{% docs description__mrt_marketing__daily_install_acquisition_campaign %}

The `mrt_marketing__daily_install_acquisition_campaign` table provides a consolidated view of daily advertising costs and user attribution metrics. It aggregates key campaign performance indicators, including advertising expenditures, app installations, user registrations, and beneficiary counts across different age groups. By selecting the most recent data per app_install_date, the model enables analysis of user acquisition performance and campaign effectiveness across media sources, campaigns, and individual advertisements.
Reporting on campaign, user attribution, and install cost is supported by the model for a defined 14-day attribution window. Key logic includes combining cost and install data from the int_appsflyer__daily_install_cost model with user event data from the int_appsflyer__daily_install_attribution model, aggregating user attribution metrics by campaign attributes and ensuring accurate reporting by selecting only the most recent execution for each app_install_date.


{% enddocs %}

## Table description

{% docs table__mrt_marketing__daily_install_acquisition_campaign %}{% enddocs %}
