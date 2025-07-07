---
title: Models overview
description: Description the distinct models in the data project.
---

## Introduction

The Pass Culture data project is a central platform for managing and analyzing data from various sources. It transforms raw data into organized formats using dbt, making it easy to use for reporting, analytics, and machine learning.

The project structure moves data through three stages: raw sources, intermediate models by source, and final global models (marts), which are the primary data sources for the analytics team. It also supports reverse ETL to share data with internal systems like ClickHouse, and backend services.

This data warehouse is essential for analytics, managed by the data analysts, and for machine learning systems maintained by the data science team. The entire data infrastructure is built and maintained by the data engineering team.

## Applicative Database

Theses models comes from our internal systems and handle the business logic of the application. The global models are an
aggregated view of intermediate models and are used as main data sources for the analytics team.

- **[Global User](models/mart/global/description__mrt_global__user.md)**: Aggregates user-related data, including
demographics, activity, and financial interactions.

- **[Global Deposit](models/mart/global/description__mrt_global__deposit.md)**: Handle the deposit amounts given to each
user. It tracks spending and booking activity.

- **[Global Offer](models/mart/global/description__mrt_global__offer.md)**: Contains detailed information about cultural
offers, including identifiers, descriptions, categories, and metadata.

- **[Global Booking](models/mart/global/description__mrt_global__booking.md)**: Tracks user bookings, including details
about the booking status, amount, and associated offers.

- **[Global Stock](models/mart/global/description__mrt_global__stock.md)**: Manages inventory levels for offers,
tracking availability, pricing, and booking limits.

## Tracking Native Tables

The following models models that comes from our tracking systems from the mobile and web application. They are used to
track user interactions within the application.

## Code & SQL

Not all models are (yet) fully described here, as our data warehouse holds an extensive collection of models.

If you want to have a look at the documentation or sql, please refer [at the
code](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models).
