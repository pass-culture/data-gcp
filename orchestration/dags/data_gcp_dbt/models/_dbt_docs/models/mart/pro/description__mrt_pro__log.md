---
title: Pro Log
description: Description of the `mrt_pro__log` table.
---

{% docs description__mrt_pro__log %}

The `mrt_pro__log` table captures detailed log data from our BtoB website ("portail pro"), the platform enabling cultural partners to manage their business activity on pass Culture. As our tracking data (mrt_pro_events table) is subject to cookies, we collect these backend logs in order to have exhaustive data on a limited number of actions required to monitor product performance and support fraud teams : user reviews, stock/offer/booking updates...

{% enddocs %}

## Table description

Detail of logs supported (field message) :
            "Booking has been cancelled"
            "Offer has been created"
            "Offer has been updated"
            "Booking was marked as used"
            "Booking was marked as unused"
            "Successfully updated stock"
            "Deleted stock and cancelled its bookings"
            "Some provided eans were not found" : as part of offer manual creation and offer synchronisation with API, our teams need to detect EANs which do not belong to our database
            "Stock update blocked because of price limitation" : fraud teams need to detect fraud attempt from cultural parterns who try to raise their price
            "User with new nav activated submitting review" and "User submitting review" : product teams gather user reviews enabled on the website
            "Offer Categorisation Data API" : data science team need to measure performance of their predictive model for individual offer creation (suggestion of subcategories)
            "Searching for structure"
            "Creating new Offerer and Venue"

{% docs table__mrt_pro__log %}{% enddocs %}
