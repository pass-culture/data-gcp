# Table: Cultural Partner

The `mrt_global__cultural_partner` table is designed to store information about the cultural partner concept that is between venue and offerer concept. A cultural partner is an in-house concept created to better record and monitor the activity of cultural institutions and companies registered on the pass Culture.

Cultural partners include all permanent venues (facilities which are opened to the public and belong to the institution) as well as cultural entities which have no permanent venue (which are not open to the public nor belong to the institution).

Example: The Opéra National de Paris structure corresponds to 3 institutions opened to the public and therefore 3 cultural partners. The We Love Green Festival corresponds to several non-permanent venues, not owned by the festival, and is therefore a single cultural partner.

A structure can therefore have several cultural partners. Local authorities are not cultural partners. Only its facilities opened to the public are (library, museum, castle...).

## Table description

| name                                    | data_type | description                                                                                                                      |
| --------------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------- |
| venue_id                                |           | Unique identifier for the venue.                                                                                                 |
| offerer_id                              |           | Unique identifier of the offerer.                                                                                                |
| partner_id                              |           | Unique identifier of the partner.                                                                                                |
| partner_creation_date                   |           | Creation date of the cultural partner (offerer or venue).                                                                        |
| was_registered_last_year                |           | Boolean for analytical purpose. On year between registration of partner (creation of the venue or the offerer) and current date. |
| partner_name                            |           | Name of the cultural partner.                                                                                                    |
| partner_academy_name                    |           | Name of the academy associated with the cultural partner.                                                                        |
| partner_region_name                     |           | Name of the region where the cultural partner is located.                                                                        |
| partner_department_code                 |           | Code of the department where the cultural partner is located.                                                                    |
| partner_department_name                 |           | Name of the department where the cultural partner is located.                                                                    |
| partner_postal_code                     |           | Postal code of the cultural partner's location.                                                                                  |
| partner_type                            |           | Type of the cultural partner, derived from venue type or venue/offerer tags.                                                     |
| partner_type_origin                     |           | Origin of the partner type, indicating whether it is derived from venue types, venue tags or offerer tags.                       |
| cultural_sector                         |           | Cultural sector associated with the partner type.                                                                                |
| dms_accepted_at                         |           | Date when the offerer was accepted in DMS.                                                                                       |
| first_dms_adage_status                  |           | First DMS adage status of the offerer.                                                                                           |
| is_reference_adage                      |           | Indicates if the offerer is a reference in adage.                                                                                |
| is_synchro_adage                        |           | Indicates if the offerer is synchronized with adage.                                                                             |
| is_active_last_30days                   |           | Analytical field: Indicates if it was active in the last 30 days.                                                                |
| is_active_current_year                  |           | Analytical field: Indicates if it is active in the current year.                                                                 |
| is_individual_active_last_30days        |           | Analytical field: Indicates if it had individual activity in the last 30 days.                                                   |
| is_individual_active_current_year       |           | Analytical field: Indicates if it has individual activity in the current year.                                                   |
| is_collective_active_last_30days        |           | Analytical field: Indicates if it had collective activity in the last 30 days.                                                   |
| is_collective_active_current_year       |           | Analytical field: Indicates if it has collective activity in the current year.                                                   |
| total_created_individual_offers         |           | Total number of individual offers created.                                                                                       |
| total_created_collective_offers         |           | Total number of collective offers created.                                                                                       |
| total_created_offers                    |           | Total number of offers created, both individual and collective.                                                                  |
| first_offer_creation_date               |           | Date of the first offer creation.                                                                                                |
| first_individual_offer_creation_date    |           | Date of the first individual offer creation.                                                                                     |
| first_collective_offer_creation_date    |           | Date of the first collective offer creation.                                                                                     |
| last_bookable_offer_date                |           | Date of the last bookable offer.                                                                                                 |
| first_bookable_offer_date               |           | Date of the first bookable offer.                                                                                                |
| first_individual_bookable_offer_date    |           | Date of the first individual bookable offer.                                                                                     |
| last_individual_bookable_offer_date     |           | Date of the last individual bookable offer.                                                                                      |
| first_collective_bookable_offer_date    |           | Date of the first collective bookable offer.                                                                                     |
| last_collective_bookable_offer_date     |           | Date of the last collective bookable offer.                                                                                      |
| total_non_cancelled_individual_bookings |           | Total number of non-cancelled individual bookings made by the user.                                                              |
| total_used_individual_bookings          |           | Total number of used individual bookings.                                                                                        |
| total_non_cancelled_collective_bookings |           | Total number of collective bookings that were not cancelled.                                                                     |
| total_used_collective_bookings          |           | Total number of collective bookings that were used.                                                                              |
| total_individual_real_revenue           |           | Total actual revenue from individual bookings.                                                                                   |
| total_collective_real_revenue           |           | Total actual revenue from collective bookings.                                                                                   |
| total_real_revenue                      |           | Total actual revenue from all bookings.                                                                                          |
| partner_status                          |           | Status of the cultural partner, indicating whether it is a (permanent) venue or an offerer (without permanent venue).            |
| partner_epci                            |           | name of the EPCI where the cultural partner is located.                                                                          |
| partner_city                            |           | name of the city where the cultural partner is located.                                                                          |
