---
title: Stock
description: Description of the `mrt_global__stock` table.
---

{% docs description__mrt_global__stock %}

# Table: Stock

The `mrt_global__stock` table provides a detailed view of stock-related data, capturing essential information about stock availability, pricing,
and associated offers. A stock has one quantity : available quantity of goods or tickets (can be null if the quantity is unlimited - ex : digital goods),
a price, and (only for events) a date.
One stock is linked to one offer, but one offer can be linked to several stocks, because an offer can have
several prices (ex : some theaters has different prices for a single show), several dates (ex : a cinema has different screenings for one movie).

{% enddocs %}



## Table description

{% docs table__mrt_global__stock  %}
{% enddocs %}
