---
title: User Beneficiary Information History
description: Description of the `mrt_global__user_beneficiary_information_history` table.
---

{% docs description__mrt_global__user_beneficiary_information_history %}

# Table: User Beneficiary Information History

The `mrt_global__user_beneficiary_information_history` table tracks historical changes to user beneficiary information over time.

{% enddocs %}

This table captures when users modify their profile information (activity status, address, city, postal code), allowing analysis of user behavior patterns while protecting privacy by excluding PII fields.

## Key Features

- Tracks activity changes (e.g., student to unemployed)
- Includes modification flags for each field type
- Maintains geographical aggregations (EPCI, IRIS, QPV) without exposing precise addresses
- Includes density and region information for demographic analysis
- Tracks user age at the time of information creation
- Supports temporal analysis with creation_timestamp and info_history_rank

## Privacy

This table excludes user_address, user_city, user_postal_code, and coordinates to protect PII.

## Table description

{% docs table__mrt_global__user_beneficiary_information_history %}{% enddocs %}
