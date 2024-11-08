---
title: Stock
description: Description of the `mrt_global__stock` table.
---

{% docs description__mrt_global__stock %}

# Table: Stock

The `mrt_global__stock` table provides a detailed view of stock-related data, capturing essential information about stock availability, pricing, 
and associated offers. 

## Main Fields
- **stock_id**: Unique identifier for the stock.
- **stock_beginning_date**: Timestamp of the beginning of the event. Only for event offers.
- **stock_price**: Price of the stock.
- **stock_quantity**: Total quantity that had been available when the stock is created. Constant. If null, the quantity is unlimited (ex : digital offers).
- **offer_id**: Identifier for the offer associated with the stock.
- **stock_last_provider_id**: Identifier of the provider that synchronised this stock. Only for the synchronised offers.
- **stock_booking_limit_date**: Timestamp that specifies when it is no longer possible to book the offer linked to this stock.
- **stock_creation_date**: Creation date of the stock.
- **stock_features**:  Movie caracteristics only for synchronised cinema screening offers (ex : VO, VF, 3D). Can be a list if several features applies to the stock.
- **stock_price**: Price of the stock. O if free.
- **total_available_stock**: Quantity of remaining stock. (stock_quantity - non cancelled bookings)
- **price_category_id**: Unique identifier of the price category.
- **price_category_label**: Name of the price category. Description written by the cultural partner of the price category of this stock (ex : "Pass 2 jours", "Prix +18 ans"). Null if there is no specific price category for the offer.
- **price_category_label_id**: Unique identifier of the price category label.
- 
{% enddocs %}



## Table description

{% docs table__mrt_global__stock  %}
{% enddocs %}
