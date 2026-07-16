# Table: Offer

This model exports offers data for ministry use. It contains information about individual cultural offers including:

- Offer details (ID, name, description, category, subcategory)
- Offer characteristics (duo, bookable, digital/physical goods, event)
- Temporal information (creation, update, publication dates)
- Status information (synchronization, national scope, active status)
- Related entities (offerer, venue, product, item) An offer in our model is a set product (identified via an offer_id) sold by a set cultural partner. For exemple, a set book (identified via its EAN) sold by a set cultural partner.

## Table description

| name                    | data_type | description                                                                                                                                                        |
| ----------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| offer_id                | STRING    | Unique identifier for the offer.                                                                                                                                   |
| offer_is_synchronised   |           | Indicates whether the offer is synchronized with API systems and has a product_id.                                                                                 |
| offer_name              |           | Name of the offer as it appears in the application.                                                                                                                |
| offer_description       |           | Offer description (synopsis, further details on the show) as provided by thecultural partner and displayed in app.                                                 |
| offer_subcategory       |           | Identifier for the subcategory of the offer. Determined by the cultural partner via a list of pre-set options.                                                     |
| offer_category          |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu.                                    |
| offer_created_at        |           | Timestamp when the offer was created.                                                                                                                              |
| offer_updated_at        |           | Timestamp when the offer was last updated.                                                                                                                         |
| offer_is_duo            |           | Indicates if the offer can be booked as a duo.                                                                                                                     |
| offer_is_bookable       |           | Indicates if the offer is bookable.                                                                                                                                |
|                         |           |                                                                                                                                                                    |
|                         |           | An offer is considered bookable if it meets the following criteria:                                                                                                |
|                         |           | 1. **Stock availability**: The offer has non-expired and non-depleted stock.                                                                                       |
|                         |           | 2. **User access**: It can be booked by users in the app.                                                                                                          |
|                         |           | 3. **Offerer Validation**: The offerer who created the offer must be officially validated.                                                                         |
| offer_is_digital_goods  |           | Indicates if the offer includes digital goods.                                                                                                                     |
| offer_is_physical_goods |           | Indicates if the offer includes physical goods.                                                                                                                    |
| offer_is_event          |           | Indicates if the offer is an event.                                                                                                                                |
| webapp_url              |           | URL to the offer on the web application.                                                                                                                           |
| offer_url               |           | URL to the offer.                                                                                                                                                  |
| offer_is_national       |           | Indicates if the offer is available nationally. This Information is originally filled by pass Culture teams but is no longer used. It will be decommissioned soon. |
| offer_is_active         |           | Indicates if the offer is active (the offer has been deactivated and is no longer visible in app).                                                                 |
| offerer_address_id      |           | The unique identifier for the mapping between an offerer and an address where he created offers.                                                                   |
| offer_publication_date  |           | Publication date of the offer on the app, bookable or not (coming soon). Data available only from July 2025.                                                       |
| offer_product_id        |           | Identifier for the product associated with the offer.                                                                                                              |
| item_id                 |           | Identifier for the item associated with the offer used internally by the data science team.                                                                        |
| venue_id                |           | Unique identifier for the venue.                                                                                                                                   |
| offerer_id              |           | Unique identifier of the offerer.                                                                                                                                  |
| offer_validation        |           | Current status of the offer validation by internal teams. The offer may be:                                                                                        |
|                         |           | - "DRAFT" (yet to be published by the cultural partner)                                                                                                            |
|                         |           | - "PENDING" (published, yet to be reviewed by the pass Culture Fraud team)                                                                                         |
|                         |           | - "VALIDATED" (validated by pass Culture fraud team, ready to be made available to users)                                                                          |
|                         |           | - "REJECTED" (rejected and not made available to users)                                                                                                            |
