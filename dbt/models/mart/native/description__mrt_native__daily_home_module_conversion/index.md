# Table: Native Daily Home Module Conversion

The `mrt_native__daily_home_module_conversion` table provides detailed insights into user interactions with the home modules on a daily basis. It captures various user activities related to the home modules, including the consultations, bookings and diversity increments.

## Table description

| name                             | data_type | description                                                                                                                     |
| -------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| module_displayed_date            |           | The date when the module was displayed to the user.                                                                             |
| module_id                        |           | The identifier for the module associated with the consultation.                                                                 |
| module_name                      |           | The name of the module in which the entry is displayed.                                                                         |
| module_type                      |           | The type of the module in which the entry is displayed (algolia, recommendation, video...)                                      |
| entry_id                         |           | The identifier for the entry associated with the consultation.                                                                  |
| entry_name                       |           | The name of the entry displayed to the user.                                                                                    |
| parent_module_id                 |           | The identifier for the parent module of the current module.                                                                     |
| parent_module_type               |           | The type of the parent module.                                                                                                  |
| parent_entry_id                  |           | The identifier for the parent entry of the current entry.                                                                       |
| typeform_id                      |           | Unique typeform_id related to a unique special event id. This id is used to link contentful datas .                             |
| app_version                      |           | The version of the application where the event was recorded.                                                                    |
| parent_home_type                 |           | The type of home associated with the parent entry.                                                                              |
| home_audience                    |           | The audience type for the home.                                                                                                 |
| user_lifecycle_home              |           | The lifecycle stage of the user in relation to the home.                                                                        |
| home_type                        |           | The type of home associated with the entry.                                                                                     |
| playlist_type                    |           | The type of playlist associated with the entry.                                                                                 |
| offer_category                   |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu. |
| playlist_reach                   |           | The reach of the playlist associated with the entry.                                                                            |
| playlist_recurrence              |           | The recurrence pattern of the playlist.                                                                                         |
| user_role                        |           | Role assigned to the user (GRANT_18, GRANT_15_17, PRO, ADMIN).                                                                  |
| total_session_display            |           | The total number of sessions with a display of a specific object like module_id or offer_id.                                    |
| total_session_with_click         |           | The total number of sessions with a click on a specific object like module_id or offer_id.                                      |
| total_sesh_consult_offer         |           | The total number of sessions with a consultation related to a specific object like module_id or offer_id.                       |
| total_session_fav                |           | The total number of sessions which puts an offer to favorite, related to a specific object like module_id or offer_id.          |
| total_session_with_consult_video |           | The total number of sessions with a video consultation, related to a specific object like module_id.                            |
| total_click                      |           | The total number of clicks on a specific object like module_id or offer_id.                                                     |
| total_consult_offer              |           | The total number of offer consultations.                                                                                        |
| total_fav                        |           | The total number of bookmark actions.                                                                                           |
| total_session_with_booking       |           | The total number of session which performed a booking.                                                                          |
| total_bookings                   |           | Total number of bookings, both individual and collective.                                                                       |
| total_non_cancelled_bookings     |           | Total number of bookings that were not cancelled, both individual and collective.                                               |
| total_diversification            |           | Total du score de diversité incrémenté par les réservations associées.                                                          |
