---
title: Models overview
description: Description the distinct models in the data project.
---

## Introduction


The Pass Culture data project is a comprehensive data management and analytics platform designed to support cultural offers. It leverages dbt to transform raw data into structured formats, facilitating efficient querying and reporting. The project organizes data into marts, focusing on key areas such as users, offers, bookings, stock, and deposits.


## Applicative Database

Theses models comes from our internal systems and handle the business logic of the application. The global models are an aggregated view of intermediate models and are used as main data sources for the analytics team.


- **[Global User](models/mart/global/description__mrt_global__user.md)**: Aggregates user-related data, including demographics, activity, and financial interactions.

- **[Global Deposit](models/mart/global/description__mrt_global__deposit.md)**: Handle the deposit amounts given to each user. It tracks spending and booking activity.

- **[Global Offer](models/mart/global/description__mrt_global__offe.md)**: Contains detailed information about cultural offers, including identifiers, descriptions, categories, and metadata.

- **[Global Booking](models/mart/global/description__mrt_global__booking.md)**: Tracks user bookings, including details about the booking status, amount, and associated offers.

- **[Global Stock](models/mart/global/description__mrt_global__stock.md)**: Manages inventory levels for offers, tracking availability, pricing, and booking limits.

## Tracking Native Tables

The following models models that comes from our tracking systems from the mobile and web application. They are used to track user interactions within the application.

## Code & SQL

Not all models are fully described here as we handle more than 1K models our DBT project.

If you want to have a look at the documentation or sql, please refer [at the code](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models).
