# Table: Stock

This model exports offer stock data for ministry use. It contains information about offer stock including:

- Stock details (ID, quantity, price, features)
- Temporal information (beginning date, booking limit date, creation date)
- Provider information (offerer)
- Related entities (offer, offerer)
- Price category information (ID, label)

## Table description

| name                     | data_type | description                                                                                                                                                                                                    |
| ------------------------ | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| stock_id                 |           | Unique identifier for the stock.                                                                                                                                                                               |
| stock_beginning_date     |           | Timestamp of the beginning of the event. Only for event offers.                                                                                                                                                |
| stock_last_provider_id   |           | Identifier of the provider that synchronised this stock. Only for the synchronised offers.                                                                                                                     |
| stock_booking_limit_date |           | Timestamp that specifies when it is no longer possible to book the offer linked to this stock.                                                                                                                 |
| stock_creation_date      |           | Creation date of the stock.                                                                                                                                                                                    |
| stock_features           |           | Movie features only for synchronised cinema screening offers (ex : VO, VF, 3D). Can be a list if several features applies to the stock.                                                                        |
| stock_price              |           | Price of the stock. O if free.                                                                                                                                                                                 |
| stock_quantity           |           | Total quantity that had been available when the stock is created. Constant. If null, the quantity is unlimited (ex : digital offers).                                                                          |
| offer_id                 |           | Unique identifier for the offer.                                                                                                                                                                               |
| offerer_id               |           | Unique identifier of the offerer.                                                                                                                                                                              |
| price_categoryId         |           | Identifier for the price category.                                                                                                                                                                             |
| price_category_label     |           | Label of the price category. Description written by the cultural partner of the price category of this stock (ex : "Pass 2 jours", "Prix +18 ans"). Null if there is no specific price category for the offer. |
