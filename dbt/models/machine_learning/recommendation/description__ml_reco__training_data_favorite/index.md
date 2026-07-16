# Table: Training Data Favorite

The `ml_reco__training_data_favorite` table contains the offer favorite information for the Two Tower training (not used currently).

## Table description

| name                 | data_type | description                                                                                                                     |
| -------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| user_id              |           | Unique identifier for a user.                                                                                                   |
| user_age             |           | Current age of the user.                                                                                                        |
| event_type           |           | The type of the interaction event (booking / click / favorite).                                                                 |
| event_date           |           | The date when the event occurred, used for partitioning the data.                                                               |
| event_hour           |           | The hour of the interaction event.                                                                                              |
| event_day            |           | The day of the interaction event.                                                                                               |
| event_month          |           | The month of the interaction event.                                                                                             |
| item_id              |           | Identifier for the item associated with the offer used internally by the data science team.                                     |
| offer_subcategory_id |           | Identifier for the subcategory of the offer. Determined by the cultural partner via a list of pre-set options.                  |
| offer_category_id    |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu. |
| genres               |           | Genres of the film, if applicable.                                                                                              |
| rayon                |           | Literary genre, if applicable.                                                                                                  |
| type                 |           | Type of the offer.                                                                                                              |
| venue_id             |           | Unique identifier for the venue.                                                                                                |
| venue_name           |           | Name of the venue.                                                                                                              |
