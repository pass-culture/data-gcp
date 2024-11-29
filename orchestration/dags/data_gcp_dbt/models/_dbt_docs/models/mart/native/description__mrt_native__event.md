---
title: Native Event
description: Description of the `mrt_native__event` table.
---

{% docs description__mrt_native__event %}

The `mrt_native__event` table captures detailed event data (screens seen, clicks, scrolls) from pass Culture native application and web app.

{% enddocs %}

Only data from users who have accepted tracking cookies (approximately 75% of traffic) are recorded in this table. Data is tracked for various user categories:

- Beneficiary users: users who have received a grant.
- General public users: users who have created an account but have not received a grant.
- Non-registered or non-logged-in users: users browsing without an account or without being logged in.

Each row in this table corresponds to an event, an action triggered by the user while navigating the application.

The key actions, listed in the event_name column, include (but are not limited to):

- ConsultOffer: the user views an offer page.
- BookingConfirmation: the user makes a booking.
- PerformSearch: the user performs a search (e.g., for offers or locations).
- ConsultVenue: the user views a partner venue's page.
- ModuleDisplayedOnHomePage: a playlist (of offers or partner venues) is displayed on the application's homepage.
- ConsultVideo: the user views a video content.

For each recorded key event, additional event parameters are also stored to provide more context about the action. These parameters can include unique identifiers (e.g., the booking_id of the booking made, the offer_id of the offer viewed, etc.) or the screen from which the action was initiated (column origin).

## Table description

{% docs table__mrt_native__event %}{% enddocs %}
