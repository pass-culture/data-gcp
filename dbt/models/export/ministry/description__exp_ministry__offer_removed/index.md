# Table: Offer Removed

This model exports offers removed data for ministry use. It contains only removed offers not longer available for booking in pass Culture apps. These offers are actually "useless" offers :

- They have never been booked
- They have never been bookable
- They haven't been modified in the year preceding their removal.

It contains information about individual cultural offers including:

- Offer details (ID, name, description, subcategory)
- Temporal information (creation, update, publication dates)
- Status information (synchronization, national scope, active status)
- Related entities (venue, product)

## Table description

| name                   | data_type | description                                                                                                                                                        |
| ---------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| offer_id               | STRING    | Unique identifier for the offer.                                                                                                                                   |
| offer_name             |           | Name of the offer as it appears in the application.                                                                                                                |
| offer_description      |           | Offer description (synopsis, further details on the show) as provided by thecultural partner and displayed in app.                                                 |
| offer_subcategory      |           | Identifier for the subcategory of the offer. Determined by the cultural partner via a list of pre-set options.                                                     |
| offer_created_at       |           | Timestamp when the offer was created.                                                                                                                              |
| offer_updated_at       |           | Timestamp when the offer was last updated.                                                                                                                         |
| offer_is_duo           |           | Indicates if the offer can be booked as a duo.                                                                                                                     |
| offer_url              |           | URL to the offer.                                                                                                                                                  |
| offer_is_national      |           | Indicates if the offer is available nationally. This Information is originally filled by pass Culture teams but is no longer used. It will be decommissioned soon. |
| offer_is_active        |           | Indicates if the offer is active (the offer has been deactivated and is no longer visible in app).                                                                 |
| offerer_address_id     |           | The unique identifier for the mapping between an offerer and an address where he created offers.                                                                   |
| offer_publication_date |           | Publication date of the offer on the app, bookable or not (coming soon). Data available only from July 2025.                                                       |
| offer_product_id       |           | Identifier for the product associated with the offer.                                                                                                              |
| venue_id               |           | Unique identifier for the venue.                                                                                                                                   |
| offer_validation       |           | Current status of the offer validation by internal teams. The offer may be:                                                                                        |
|                        |           | - "DRAFT" (yet to be published by the cultural partner)                                                                                                            |
|                        |           | - "PENDING" (published, yet to be reviewed by the pass Culture Fraud team)                                                                                         |
|                        |           | - "VALIDATED" (validated by pass Culture fraud team, ready to be made available to users)                                                                          |
|                        |           | - "REJECTED" (rejected and not made available to users)                                                                                                            |
