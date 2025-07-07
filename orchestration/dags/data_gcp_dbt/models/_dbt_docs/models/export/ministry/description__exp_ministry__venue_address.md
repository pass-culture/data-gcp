---
title: Venue Address
description: Description of the `exp_ministry__venue_address` table.
---

{% docs description__exp_ministry__venue_address %}

# Table: Venue Address

This model exports venue address data for ministry use.
It contains information about administrative venue locations.
Currently this concept should be preferred to the new address concept that is still under development.
- Basic address information (street, city, postal code)
- Geographic coordinates (latitude, longitude)
- Administrative information (department code, region name)
- QPV (Quartier Prioritaire de la Ville) information (code and name)
- IRIS internal ID (unique identifier of the IRIS) that can be used to join with the IRIS data provided.

{% enddocs %}

## Table description

{% docs table__exp_ministry__venue_address %}{% enddocs %}
