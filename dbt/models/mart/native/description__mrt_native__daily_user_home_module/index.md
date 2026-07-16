# Table: Daily User Home Module

The `mrt_native__daily_user_home_module` table provides detailed insights into user interactions with home modules on a daily basis. It captures various user activities, module metadata, and associated timestamps, enabling comprehensive analysis of user behavior and module performance.

## Table description

| name                       | data_type | description                                                                                                                                   |
| -------------------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| user_id                    |           | Unique identifier for a user.                                                                                                                 |
| unique_session_id          |           | A unique identifier for the session, ensuring no duplicates.                                                                                  |
| entry_id                   |           | The identifier for the entry associated with the consultation.                                                                                |
| entry_name                 |           | The name of the entry displayed to the user.                                                                                                  |
| module_id                  |           | The identifier for the module associated with the consultation.                                                                               |
| module_name                |           | The name of the module in which the entry is displayed.                                                                                       |
| typeform_id                |           | Unique typeform_id related to a unique special event id. This id is used to link contentful datas .                                           |
| parent_module_id           |           | The identifier for the parent module of the current module.                                                                                   |
| parent_module_type         |           | The type of the parent module.                                                                                                                |
| parent_entry_id            |           | The identifier for the parent entry of the current entry.                                                                                     |
| parent_home_type           |           | The type of home associated with the parent entry.                                                                                            |
| module_type                |           | The type of the module in which the entry is displayed (algolia, recommendation, video...)                                                    |
| user_location_type         |           | The type of geolocation used by the user during the session (via geolocation data, manually entered by the user, or information unavailable). |
| reco_call_id               |           | The identifier for the recommendation call.                                                                                                   |
| app_version                |           | The version of the application where the event was recorded.                                                                                  |
| click_type                 |           | The type of click interaction by the user.                                                                                                    |
| offer_id                   |           | Unique identifier for the offer.                                                                                                              |
| venue_id                   |           | Unique identifier for the venue.                                                                                                              |
| booking_id                 |           | Unique identifier for a booking.                                                                                                              |
| module_displayed_date      |           | The date when the module was displayed to the user.                                                                                           |
| module_displayed_timestamp |           | The timestamp when the module was displayed to the user.                                                                                      |
| module_clicked_timestamp   |           | The timestamp when the module was clicked by the user.                                                                                        |
| consult_venue_timestamp    |           | The timestamp when the venue was consulted by the user.                                                                                       |
| consult_offer_timestamp    |           | The timestamp when the offer was consulted by the user.                                                                                       |
| fav_timestamp              |           | The timestamp when the entry was marked as favorite by the user.                                                                              |
| booking_timestamp          |           | The timestamp when the booking was made by the user.                                                                                          |
| home_audience              |           | The audience type for the home.                                                                                                               |
| user_lifecycle_home        |           | The lifecycle stage of the user in relation to the home.                                                                                      |
| home_type                  |           | The type of home associated with the entry.                                                                                                   |
| playlist_type              |           | The type of playlist associated with the entry.                                                                                               |
| offer_category             |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu.               |
| playlist_reach             |           | The reach of the playlist associated with the entry.                                                                                          |
| playlist_recurrence        |           | The recurrence pattern of the playlist.                                                                                                       |
