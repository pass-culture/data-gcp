The `mrt_collective__adage_log` table captures detailed log data from a distant website owned by the Ministère de l'Éducation Nationale. It is in this website where teachers can consult and book collective offers.

## Table description

This table contains only informations about teachers navigation on Adage platform. Each row corresponds to an event.

| name                                  | data_type | description                                                                                                              |
| ------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------ |
| event_date                            |           | The date when the event occurred, used for partitioning the data.                                                        |
| event_name                            |           | The name of the event. For screen views, it concatenates the event name with the Firebase screen name.                   |
| event_timestamp                       |           | The exact timestamp when the event was recorded.                                                                         |
| user_id                               |           | Unique identifier for a user.                                                                                            |
| session_id                            |           | Deprecated: use unique_session_id. The session identifier during which the event was recorded.                           |
| total_results                         |           | The total number of results returned by the query associated with the event.                                             |
| origin                                |           | The origin of the event, indicating where it was triggered from.                                                         |
| collective_stock_id                   |           | Identifier for the collective stock associated with the booking.                                                         |
| collective_offer_id                   |           | Identifier for the collective offer.                                                                                     |
| header_link_name                      |           | The name of the header link clicked during the event, if applicable.                                                     |
| collective_booking_id                 |           | Unique identifier for a collective booking.                                                                              |
| query_id                              |           | The identifier for the query associated with the event.                                                                  |
| address_type_filter                   |           | The address type filter applied during the event, if applicable.                                                         |
| text_filter                           |           | The text filter applied during the event, if applicable.                                                                 |
| department_filter                     |           | The department filter applied during the event, if applicable.                                                           |
| academy_filter                        |           | The academy filter applied during the event, if applicable.                                                              |
| artistic_domain_filter                |           | The artistic domain filter applied during the event, if applicable.                                                      |
| student_filter                        |           | The student filter applied during the event, if applicable.                                                              |
| suggestion_type                       |           | The type of suggestion made during the event, if applicable.                                                             |
| suggestion_value                      |           | The value of the suggestion made during the event, if applicable.                                                        |
| is_favorite                           |           | Indicates whether the event is marked as a favorite.                                                                     |
| venue_filter                          |           | The venue filter applied during the event, if applicable.                                                                |
| format_filter                         |           | The format filter applied during the event, if applicable.                                                               |
| playlist_id                           |           | The identifier for the playlist associated with the event, if applicable.                                                |
| domain_id                             |           | The identifier for the domain associated with the event, if applicable.                                                  |
| rank_clicked                          |           | The rank of the item clicked during the event, if applicable.                                                            |
| adage_venue_id                        |           | The identifier for the venue associated with the Adage event.                                                            |
| offer_venue_id                        |           | The identifier for the venue associated with the offer.                                                                  |
| venue_id                              |           | Unique identifier for the venue.                                                                                         |
| partner_id                            |           | Unique identifier of the partner.                                                                                        |
| offer_students                        |           | The number of students associated with the collective offer.                                                             |
| offer_format                          |           | The format of the collective offer associated with the event.                                                            |
| partner_name                          |           | Name of the cultural partner.                                                                                            |
| partner_type                          |           | Type of the cultural partner, derived from venue type or venue/offerer tags.                                             |
| partner_status                        |           | Status of the cultural partner, indicating whether it is a (permanent) venue or an offerer (without permanent venue).    |
| cultural_sector                       |           | Cultural sector associated with the partner type.                                                                        |
| partner_confirmed_collective_bookings |           | The total number of confirmed collective bookings by the partner.                                                        |
| partner_department_code               |           | Code of the department where the cultural partner is located.                                                            |
| uai                                   |           | The UAI (Unique Academic Identifier) associated with the event.                                                          |
| user_role                             |           | Role assigned to the user (GRANT_18, GRANT_15_17, PRO, ADMIN).                                                           |
| geoloc_radius_filter                  |           | The distance filter applied during the search event (maximal radius around the school, km), if applicable (facultative). |
| source                                |           | When the event OfferListSwitch is triggered, it specifies from the initial offer list view (grid, map).                  |
