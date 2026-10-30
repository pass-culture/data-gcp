---
title: Transactional pro
description: Description of the `mrt_marketing__transactional_pro` table.
---

{% docs description__mrt_marketing__transactional_pro %}

The `mrt_marketing__transactional_pro` table aggregates Brevo transactional email events sent to cultural partners. Each row represents one tag, template, offerer and event day. The boolean columns are true when at least one recipient of the offerer was delivered, opened or unsubscribed; the `total_recipients_*` columns count the distinct recipients who did.

This dataset can be used to monitor the effectiveness of email campaigns, evaluate offerer behavior, and identify trends or opportunities to enhance engagement strategies.

{% enddocs %}

## Table description

{% docs table__mrt_marketing__transactional_pro %}{% enddocs %}
