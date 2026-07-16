# Table: Training Data User Feature

The `ml_reco__training_data_user_feature` table contains the user features used during the Two Tower training.

## Table description

| name                              | data_type | description                                                                                                                                                                                                                                      |
| --------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| user_id                           |           | Unique identifier for a user.                                                                                                                                                                                                                    |
| consult_offer                     |           | The total number of items consulted by the user.                                                                                                                                                                                                 |
| booking_cnt                       |           | Total number of bookings, both individual and collective.                                                                                                                                                                                        |
| user_theoretical_remaining_credit |           | Total theoretical remaining credit for the user.                                                                                                                                                                                                 |
| has_added_offer_to_favorites      |           | Boolean. Indicates if the user has added any offer to their favorites.                                                                                                                                                                           |
| user_queries                      |           | The search query associated with the event, if applicable.                                                                                                                                                                                       |
| qpi_subcategory_ids               |           | QPI stands for 'Initial Practice Questionnaires'. We asked young users about their cultural practices before using the Pass, resulting in a list of subcategories used during the cold start to display offers based on these initial practices. |
