---
title: Offerer
description: Description of the `mrt_global__offerer` table.
---

{% docs description__mrt_global__offerer %}

# Offerer Model

The `mrt_global__offerer` table aims to list all the offerer (identifiable by a SIREN) registered on the Culture Pass, with information on:

- The nature of the offerer (name, geographical location, legal status)
- The activities of this offerer (creation of offers / bookings / revenue across each category)

\
**Business Rules**
- Only active offerer (is_active = TRUE)
- Only offerer approved through certification (validation_status = 'VALIDATED')
- All activation statuses (including offerer that have never published an offer)



{% enddocs %}

## Table description

{% docs table__mrt_global__offerer  %}{% enddocs %}
