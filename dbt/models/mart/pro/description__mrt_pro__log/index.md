The `mrt_pro__log` table captures detailed log data from our BtoB website ("portail pro"), the platform enabling cultural partners to manage their business activity on pass Culture. As our tracking data (mrt_pro_events table) is subject to cookies, we collect these backend logs in order to have exhaustive data on a limited number of actions required to monitor product performance and support fraud teams : user reviews, stock/offer/booking updates...

## Table description

Detail of logs supported (field message) : "Booking has been cancelled" "Offer has been created" "Offer has been updated" "Booking was marked as used" "Booking was marked as unused" "Successfully updated stock" "Deleted stock and cancelled its bookings" "Some provided eans were not found" : as part of offer manual creation and offer synchronisation with API, our teams need to detect EANs which do not belong to our database "Stock update blocked because of price limitation" : fraud teams need to detect fraud attempt from cultural parterns who try to raise their price "User with new nav activated submitting review" and "User submitting review" : product teams gather user reviews enabled on the website "Offer Categorisation Data API" : data science team need to measure performance of their predictive model for individual offer creation (suggestion of subcategories) "Searching for structure" "Creating new Offerer and Venue" Several engagement actions (editorialisation of the cultural partner via "Espace partenaire")

1. Video on offer "Video has been deleted from offer" "Video has been added to offer" "Video has been updated on offer"
1. Application to highlight playlists on Home "Highlight requests have been created" "Highlight requests have been deleted"
1. Recommendation of an offer "Pro advice updated" "Pro advice created"

| name                               | data_type | description                                                                                                                  |
| ---------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------- |
| environement                       |           | The environment in which the log entry was recorded, such as production or staging.                                          |
| user_id                            |           | Unique identifier for a user.                                                                                                |
| offerer_id                         |           | Unique identifier of the offerer.                                                                                            |
| message                            |           | The message content of the log entry, describing the event or action.                                                        |
| booking_id                         |           | Unique identifier for a booking.                                                                                             |
| offer_id                           |           | Unique identifier for the offer.                                                                                             |
| venue_id                           |           | Unique identifier for the venue.                                                                                             |
| provider_id                        |           | The unique identifier for the synchronization provider.                                                                      |
| product_id                         |           | Identifier for the product associated with the offer.                                                                        |
| stock_id                           |           | Unique identifier for the stock.                                                                                             |
| stock_old_quantity                 |           | The previous quantity of the stock before the log entry event.                                                               |
| stock_new_quantity                 |           | The new quantity of the stock after the log entry event.                                                                     |
| stock_old_price                    |           | The previous price of the stock before the log entry event.                                                                  |
| stock_new_price                    |           | The new price of the stock after the log entry event.                                                                        |
| stock_booking_quantity             |           | The quantity of stock booked during the log entry event.                                                                     |
| offerer_address_old_value          |           | The previous address (performance venue) of the offer/stock before modification, as recorded in the system.                  |
| offerer_address_new_value          |           | The new address (performance venue) of the offer/stock after modification, as updated in the system.                         |
| publication_date_old_value         |           | The previous publication date of the offer before modification.                                                              |
| publication_date_new_value         |           | The new publication date of the offer after modification.                                                                    |
| booking_limit_date_old_value       |           | The previous booking limit date of the offer before modification.                                                            |
| booking_limit_date_new_value       |           | The new booking limit date of the offer after modification.                                                                  |
| stock_beginning_date_old_value     |           | The previous beginning date of the stock before modification.                                                                |
| stock_beginning_date_new_value     |           | The new beginning date of the stock after modification.                                                                      |
| offer_withdrawal_details_old_value |           | The previous withdrawal details of the offer before modification.                                                            |
| offer_withdrawal_details_new_value |           | The new withdrawal details of the offer after modification.                                                                  |
| list_of_eans_not_found             |           | A list of EANs (European Article Numbers) that were not found during the log entry event, offer creation or synchronisation. |
| log_timestamp                      |           | The timestamp when the log entry was recorded.                                                                               |
| partition_date                     |           | The date used for partitioning the log data.                                                                                 |
| beta_test_new_nav_is_convenient    |           | Feedback on whether navigation on the new pro website is convenient, collected during beta testing (04/2024-11/2024).        |
| beta_test_new_nav_is_pleasant      |           | Feedback on whether navigation on the new pro website is pleasant, collected during beta testing (04/2024-11/2024).          |
| beta_test_new_nav_comment          |           | Textual reviews on the new pro website interface, collected from users during beta testing (04/2024-11/2024).                |
| technical_message_id               |           | The technical identifier for the message associated with the log entry.                                                      |
| choice_datetime                    |           | The timestamp when the cookie conset was recorded.                                                                           |
| device_id                          |           | The identifier for the device used during the log entry event.                                                               |
| analytics_source                   |           | The source of analytics data, such as "adage" "backoffice", "app-pro", "native" associated with the log entry.               |
| cookies_consent_mandatory          |           | Indicates whether cookies consent is mandatory for the user.                                                                 |
| cookies_consent_accepted           |           | Indicates whether the user accepted cookies consent.                                                                         |
| cookies_consent_refused            |           | Indicates whether the user refused cookies consent.                                                                          |
| user_satisfaction                  |           | Textual reviews on the pro website interface and navigation, collected from November 2024.                                   |
| user_comment                       |           | Multi-choice feedback on pro website from very bad to excellent, collected from November 2024.                               |
| suggested_offer_api_id             |           | API call ID as part of individual offer creation predictiv model of suggested subcategories.                                 |
| suggested_offer_api_subcategory    |           | Subcategory chosen by the user as part of individual offer creation(predictiv model).                                        |
| suggested_offer_api_subcategories  |           | Subcategories suggested to the user as part of individual offer creation (predictiv model).                                  |
| siret                              |           | The SIRET code by the cultural partner during subscription                                                                   |
| siret_is_diffusible                |           | Indicates whether SIRET with which the cultural partner subscribed is diffisuble.                                            |
