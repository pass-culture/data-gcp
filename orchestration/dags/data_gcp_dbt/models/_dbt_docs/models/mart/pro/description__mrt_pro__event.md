---
title: Pro Event
description: Description of the `mrt_pro__event` table.
---

{% docs description__mrt_pro__event %}

The `mrt_pro__event`table captures detailed event data (screens seen, clicks, scrolls) from pass Culture professional website 'Portail Pro' where cultural partners can manage their profile and create offers.
{% enddocs %}

## Table description

Only datas for professional users who have accepted tracking cookies (approximately 80% of traffic)
are recorded in this table. Each row in this table corresponds to an event, an action triggered by the user while navigating the website.

The key actions, listed in the event_name column, include (but are not limited to):

- hasClickedOffer: the pro user clicks on an offer to consult it.
- hasClickedShowBooking: the pro user displays a booking on one of its offers.

For each recorded key event, additional event parameters are also stored to provide more context about the action. These parameters can include unique identifiers (e.g., the offer_id of the offer viewed, etc.) or the screen from which the action was initiated (column origin).

{% docs table__mrt_pro__event %}{% enddocs %}
