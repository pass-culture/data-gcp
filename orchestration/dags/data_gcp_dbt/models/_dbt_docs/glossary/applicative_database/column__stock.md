{% docs column__stock_id %}Unique identifier for the stock.{% enddocs %}
{% docs column__stock_beginning_date %}Timestamp of the beginning of the event. Only for event offers.{% enddocs %}
{% docs column__stock_last_provider_id %}Identifier of the provider that synchronised this stock. Only for the synchronised offers.{% enddocs %}
{% docs column__stock_booking_limit_date %}Timestamp that specifies when it is no longer possible to book the offer linked to this stock.{% enddocs %}
{% docs column__stock_creation_date %}Creation date of the stock.{% enddocs %}
{% docs column__stock_features %}Movie features only for synchronised cinema screening offers (ex : VO, VF, 3D). Can be a list if several features applies to the stock.{% enddocs %}
{% docs column__stock_price %}Price of the stock. O if free.{% enddocs %}
{% docs column__stock_quantity %}Total quantity that had been available when the stock is created. Constant. If null, the quantity is unlimited (ex : digital offers).{% enddocs %}
{% docs column__price_category_id %}Identifier for the price category.{% enddocs %}
{% docs column__price_category_label %}Label of the price category. Description written by the cultural partner of the price category of this stock (ex : "Pass 2 jours", "Prix +18 ans"). Null if there is no specific price category for the offer.{% enddocs %}
{% docs column__price_category_label_id %}Identifier for the price category label.{% enddocs %}
{% docs column__last_stock_price %} The last recorded stock price for the offer. {% enddocs %}
{% docs column__first_stock_creation_date %} Date of the first stock creation of the offerer. {% enddocs %}
{% docs column__total_available_stock %} Quantity of remaining stock. (stock_quantity - non cancelled bookings){% enddocs %}
