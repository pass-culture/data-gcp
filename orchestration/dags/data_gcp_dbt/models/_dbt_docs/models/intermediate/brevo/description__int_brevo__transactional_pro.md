---
title: Transactional pro
description: Description of the `int_brevo__transactional_pro` table.
---

{% docs description__int_brevo__transactional_pro %}

The `int_brevo__transactional_pro` table holds event-level data from Brevo for transactional emails sent to cultural partners. Each row represents one recipient (`email_id`, a hash of the recipient address) of a template and tag, for one offerer and one event day. Counts of delivered, opened and unsubscribed events are summed over every import covering that day. A recipient address linked to several offerers appears once per offerer.

This dataset can be used to monitor the effectiveness of email campaigns, evaluate offerer behavior, and identify trends or opportunities to enhance engagement strategies.

{% enddocs %}

## Table description

{% docs table__int_brevo__transactional_pro %}{% enddocs %}
