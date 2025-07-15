---
title: Retention pro
description: Description of the `mrt_marketing__retention_pro` table.
---

{% docs description__mrt_marketing__retention_pro %}

The `mrt_marketing__retention_pro` is an offerer-level table which aims at analyzing the impact of transactional campaigns (+ newsletters soon) from Brevo on offerer retention (bookable offers and bookings).

{% enddocs %}

## Table description

Each line is retention datas of a single offerer, which received and openened at least one retention email.

Why studying retention of offerers while we mainly send campaigns to venues ?
Unfortunately, the only data provided by Brevo for a mail sent is the email address, which can either be a user email, a venue booking email, a venue collective booking email, a venue contact email... However, one email can be linked to several venues of a single offerer : that's why we cannot clearly specify which venue the mail targets. The offerer is the only granularity we can rely on.

{% docs table__mrt_marketing__retention_pro %}{% enddocs %}
