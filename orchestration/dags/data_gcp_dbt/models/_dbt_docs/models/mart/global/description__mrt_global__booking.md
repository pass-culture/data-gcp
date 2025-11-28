---
title: Booking
description: Description of the `mrt_global__booking` table.
---

{% docs description__mrt_global__booking %}

# Table: Global Booking

The `mrt_global__booking` table is designed to store comprehensive information about individual bookings.

{% enddocs %}

Currently, only users with current grant or prior grant history can make a booking.
All indvidual bookings are stored, irrespective of booking status.
A booking is made by a user (identified via a user_id), using its grant (identified by a deposit_id) towards an offer (identified by an offer_id) hosted by a cultural venue (identified by a venue_id).

## Table description

{% docs table__mrt_global__booking  %}{% enddocs %}
