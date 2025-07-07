---
title: User Address
description: Description of the `exp_ministry__user_address` table.
---

{% docs description__exp_ministry__user_address %}

# Table: User Address

This model exports beneficiary user address data for ministry use.
It contains information about declarative user locations including:
The address is the one declared by the beneficiary in their profile, and can be updated by the beneficiary.
- Geographic information (latitude, longitude, postal code)
- Address details (raw address, geocode type)
- Administrative information (academy name, department code, region name)
- QPV (Quartier Prioritaire de la Ville) information (code and name)
- IRIS internal ID (unique identifier of the IRIS) that can be used to join with the IRIS data provided.

{% enddocs %}

## Table description

{% docs table__exp_ministry__user_address %}{% enddocs %}
