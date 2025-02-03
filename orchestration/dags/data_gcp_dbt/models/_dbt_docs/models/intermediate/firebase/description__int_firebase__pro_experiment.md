---
title: Pro Experiment
description: Description of the `int_firebase__pro_experiment` table.
---

{% docs description__int_firebase__pro_experiment %}

The `int_firebase__pro_experiment` table assigns each user of the cultural partner web portal to a version of an A/B test every day, for all A/B tests in production.
{% enddocs %}

## Table description

The primary key of the table is the combination of the following columns:
- event_date: The date of the test (day).
- user_pseudo_id: A unique identifier corresponding to the user's device or browser.
- user_id: The actual user identifier.
- experiment_name: The name of the A/B test.

A single user (user_id) may be assigned to multiple test versions if they switch devices or browsers, as each user_pseudo_id is treated independently. This behavior must be taken into account when analyzing test results to avoid skewed data.

A given user_pseudo_id should not switch test versions during the same experiment. This ensures consistency within the same browser or device.

{% docs table__int_firebase__pro_experiment %}{% enddocs %}