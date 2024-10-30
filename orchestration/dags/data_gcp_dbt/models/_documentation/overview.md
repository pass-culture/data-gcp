---
title: Models overview
description: Description the distinct models in the data project.
---

## Introduction

{# this part is in dbt only #}
{% docs __overview__ %}


The Pass Culture data project is a comprehensive data management and analytics platform designed to support cultural offers. It leverages dbt to transform raw data into structured formats, facilitating efficient querying and reporting. The project organizes data into marts, focusing on key areas such as users, offers, bookings, stock, and deposits.

{% enddocs %}
{# end this #}

## Applicative Database

Theses models comes from our internal systems and handle the business logic of the application. The global models are an aggregated view of intermediate models and are used as main data sources for the analytics team.


- **[Global User](/data-gcp/dbt/models/mart/global/description__mrt_global__user/)**: Aggregates user-related data, including demographics, activity, and financial interactions.

- **[Global Deposit](/data-gcp/dbt/models/mart/global/description__mrt_global__deposit/)**: Handle the deposit amounts given to each user. It tracks spending and booking activity.

- **[Global Offer](/data-gcp/dbt/models/mart/global/description__mrt_global__offer/)**: Contains detailed information about cultural offers, including identifiers, descriptions, categories, and metadata.

- **[Global Booking](/data-gcp/dbt/models/mart/global/description__mrt_global__booking/)**: Tracks user bookings, including details about the booking status, amount, and associated offers.

- **[Global Stock](/data-gcp/dbt/models/mart/global/description__mrt_global__stock/)**: Manages inventory levels for offers, tracking availability, pricing, and booking limits.

## Tracking Native Tables

The following models models that comes from our tracking systems from the mobile and web application. They are used to track user interactions within the application.

## Code & SQL

Not all models are described here as we handle >1K models in DBT. If you want to have a look at the documentation or sql, please refer [at the code](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models).
