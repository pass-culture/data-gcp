The `mrt_native__monthly_item_discoverability` table is a one-row-per-item-per-month aggregate combining bookable-offer inventory (how many offers of an item were bookable, and on how many days) with how often that item was consulted, broken down by discovery channel (search, home, venue, favorites, similar-offer recommendations, other).

It is used to study how discoverable an item is relative to how much inventory of it is actually bookable, and through which channels users find it. An "item" is a generic content grouping used internally by the data team (`item_id`), shared across offer versions/providers of the same underlying content.

## Table description

| name                                   | data_type | description                                                                                                                                                                    |
| -------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| item_id                                |           | Identifier for the item associated with the offer used internally by the data science team.                                                                                    |
| offer_subcategory_id                   |           | Identifier for the subcategory of the offer. Determined by the cultural partner via a list of pre-set options.                                                                 |
| offer_category_id                      |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu.                                                |
| month                                  |           | First day of the calendar month the row aggregates (bookability and consultation counts are computed per month).                                                               |
| nb_bookable_offers                     |           | Number of distinct offers of this item that were bookable at least once during the month.                                                                                      |
| nb_bookable_days                       |           | Number of distinct days during the month on which at least one offer of this item was bookable.                                                                                |
| nb_monthly_consult                     |           | Total number of consultations of this item recorded during the month, all origins combined (Firebase offer-consultation events).                                               |
| nb_monthly_search_consult              |           | Number of the item's monthly consultations that originated from search.                                                                                                        |
| nb_monthly_home_consult                |           | Number of the item's monthly consultations that originated from the home page (`home`, `video`, `videoModal`, `highlightOffer`, `thematicHighlight` or `exclusivity` origins). |
| nb_monthly_venue_consult               |           | Number of the item's monthly consultations that originated from a venue page.                                                                                                  |
| nb_monthly_favorites_consult           |           | Number of the item's monthly consultations that originated from the user's favorites.                                                                                          |
| nb_monthly_similar_offer_consult       |           | Number of the item's monthly consultations that originated from a similar-offer or same-artist-playlist recommendation.                                                        |
| nb_monthly_other_channel_offer_consult |           | Number of the item's monthly consultations that originated from any channel other than search, home, venue, favorites or similar-offer recommendations.                        |
