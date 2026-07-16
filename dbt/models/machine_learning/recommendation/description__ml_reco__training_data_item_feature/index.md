# Table: Training Item Feature

The `ml_reco__training_data_item_feature` table contains the item features used during the Two Tower training.

## Table description

| name                                   | data_type | description                                                                                                                     |
| -------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| item_id                                |           | Identifier for the item associated with the offer used internally by the data science team.                                     |
| offer_category_id                      |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu. |
| offer_subcategory_id                   |           | Identifier for the subcategory of the offer. Determined by the cultural partner via a list of pre-set options.                  |
| item_image_embedding                   |           | Embedding generated with the offer image.                                                                                       |
| item_semantic_content_hybrid_embedding |           | Embedding generated with the image and the semantic content of an image.                                                        |
| item_names                             |           | Name of the offer as it appears in the application.                                                                             |
| item_descriptions                      |           | Offer description (synopsis, further details on the show) as provided by thecultural partner and displayed in app.              |
| item_rayons                            |           | Literary genre, if applicable.                                                                                                  |
| item_author                            |           | The offer's author (a book's author, a music's singer, a movie's director).                                                     |
| item_performer                         |           | Performers involved in this offer.                                                                                              |
| item_mean_stock_price                  |           | The average stock price for the item.                                                                                           |
| item_booking_cnt                       |           | Total number of bookings, both individual and collective.                                                                       |
| item_favourite_cnt                     |           | Total number of times this offer was favorited.                                                                                 |
