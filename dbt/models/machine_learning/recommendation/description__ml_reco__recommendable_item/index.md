# Table: Recommendable Item

The `ml_reco__recommendable_item` table contains the offer which can be recommended to the user with the retrieval API.

## Table description

| name                         | data_type | description                                                                                                                     |
| ---------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| item_id                      |           | Identifier for the item associated with the offer used internally by the data science team.                                     |
| topic_id                     |           | Cluster id from the `unconstrained_item_cluster`.                                                                               |
| cluster_id                   |           | Cluster id from the `default_item_cluster`.                                                                                     |
| category                     |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu. |
| subcategory_id               |           | Identifier for the subcategory of the offer. Determined by the cultural partner via a list of pre-set options.                  |
| search_group_name            |           | Legacy: Category displayed in the application                                                                                   |
| is_numerical                 |           | Indicates if the offer is digital (based on an url).                                                                            |
| is_geolocated                |           | Indicates if the offer is geolocated.                                                                                           |
| offer_is_duo                 |           | Indicates if the offer can be booked as a duo.                                                                                  |
| offer_type_domain            |           | Deprecated: The offer's category type,as many metadata info are specific to certain                                             |
|                              |           | offer types. Can be either "BOOK", "MUSIC", "SHOW" or "MOVIE".                                                                  |
| offer_type_label             |           | Deprecated: Defines the offer genre for music, books and movies. Defines the offer                                              |
|                              |           | show type for shows.                                                                                                            |
| gtl_id                       |           | Unique identifier of the Genre Tite Live (GTL) associated to this offer.                                                        |
| gtl_l1                       |           | Name of the level 1 GTL associated to this offer (for example, "Littératurefor a                                                |
|                              |           | book or "Pop" for music.)                                                                                                       |
| gtl_l2                       |           | Name of the level 2 GTL associated to this offer (for example, "Poésie" for a book                                              |
|                              |           | or "Brit Pop" for music.)                                                                                                       |
| gtl_l3                       |           | Name of the level 3 GTL associated to this offer (for example, "Haiku" for a book).                                             |
|                              |           | Only available for books.                                                                                                       |
| gtl_l4                       |           | Name of the level 4 GTL associated to this offer. Only available for books.                                                     |
| booking_number               |           | Total number of bookings for a given item.                                                                                      |
| booking_number_last_7_days   |           | Total number of bookings for a given item made during the last 7 days.                                                          |
| booking_number_last_14_days  |           | Total number of bookings for a given item made during the last 14 days.                                                         |
| booking_number_last_28_days  |           | Total number of bookings for a given item made during the last 28 days.                                                         |
| is_underage_recommendable    |           | Indicates if the item is recommendable for underage users.                                                                      |
| is_sensitive                 |           | Indicates if the item is considered sensitive.                                                                                  |
| is_restrained                |           | Indicates if the item is restrained.                                                                                            |
| offer_creation_date          |           | Date when the offer was created.                                                                                                |
| stock_beginning_date         |           | Timestamp of the beginning of the event. Only for event offers.                                                                 |
| stock_price                  |           | Price of the stock. O if free.                                                                                                  |
| total_offers                 |           | Indicates the number of offers for a give item.                                                                                 |
| semantic_emb_mean            |           | Mean value of the semantic embedding for a given item.                                                                          |
| booking_trend                |           | Trend of bookings for a given item.                                                                                             |
| stock_date_penalty_factor    |           | Penalty factor based on the stock date of the item.                                                                             |
| creation_date_penalty_factor |           | Penalty factor based on the creation date of the item.                                                                          |
| example_offer_name           |           | Example name of an offer for the item.                                                                                          |
| example_offer_id             |           | Example ID of an offer for the item.                                                                                            |
| example_venue_id             |           | Example ID of a venue for the item.                                                                                             |
| example_venue_longitude      |           | Longitude of the example venue for the item.                                                                                    |
| example_venue_latitude       |           | Latitude of the example venue for the item.                                                                                     |
| booking_release_trend        |           | Trend of booking releases for a given item.                                                                                     |
| booking_creation_trend       |           | Trend of booking creations for a given item.                                                                                    |
