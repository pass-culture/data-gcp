# Offerer Model

The `mrt_global__offerer` table aims to list all the offerer (identifiable by a SIREN) registered on the Culture Pass, with information on:

- The nature of the offerer (name, geographical location, legal status)
- The activities of this offerer (creation of offers / bookings / revenue across each category)

\
**Business Rules**

- Only active offerer (is_active = TRUE)
- Only offerer approved through certification (validation_status = 'VALIDATED')
- All activation statuses (including offerer that have never published an offer)

## Table description

| name                                    | data_type | description                                                                                                      |
| --------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------- |
| offerer_id                              |           | Unique identifier of the offerer.                                                                                |
| partner_id                              |           | Unique identifier of the partner.                                                                                |
| offerer_name                            |           | Name of the offerer.                                                                                             |
| offerer_creation_date                   |           | Date when the offerer was created. Equals to the registration date of the offerer on the professional portal.    |
| offerer_validation_date                 |           | Date when the offerer was validated (validation_status = ‘VALIDATED’).                                           |
| first_stock_creation_date               |           | Date of the first stock creation of the offerer.                                                                 |
| first_individual_offer_creation_date    |           | Date of the first individual offer creation.                                                                     |
| last_individual_offer_creation_date     |           | Date of the last individual offer creation.                                                                      |
| first_collective_offer_creation_date    |           | Date of the first collective offer creation.                                                                     |
| last_collective_offer_creation_date     |           | Date of the last collective offer creation.                                                                      |
| first_offer_creation_date               |           | Date of the first offer creation.                                                                                |
| last_offer_creation_date                |           | Date of the last offer creation.                                                                                 |
| first_individual_booking_date           |           | Date of the first individual booking.                                                                            |
| last_individual_booking_date            |           | Date of the last individual booking.                                                                             |
| first_bookable_offer_date               |           | Date of the first bookable offer.                                                                                |
| last_collective_bookable_offer_date     |           | Date of the last collective bookable offer.                                                                      |
| first_individual_bookable_offer_date    |           | Date of the first individual bookable offer.                                                                     |
| last_individual_bookable_offer_date     |           | Date of the last individual bookable offer.                                                                      |
| first_collective_bookable_offer_date    |           | Date of the first collective bookable offer.                                                                     |
| first_booking_date                      |           | Date of the first booking.                                                                                       |
| last_booking_date                       |           | Date of the last booking.                                                                                        |
| last_bookable_offer_date                |           | Date of the last bookable offer.                                                                                 |
| total_non_cancelled_individual_bookings |           | Total number of non-cancelled individual bookings made by the user.                                              |
| total_non_cancelled_collective_bookings |           | Total number of collective bookings that were not cancelled.                                                     |
| total_non_cancelled_bookings            |           | Total number of bookings that were not cancelled, both individual and collective.                                |
| total_used_bookings                     |           | Total number of bookings that were used.                                                                         |
| total_used_individual_bookings          |           | Total number of used individual bookings.                                                                        |
| total_used_collective_bookings          |           | Total number of collective bookings that were used.                                                              |
| total_individual_theoretic_revenue      |           | Total theoretical revenue from individual bookings.                                                              |
| total_individual_real_revenue           |           | Total actual revenue from individual bookings.                                                                   |
| total_collective_theoretic_revenue      |           | Total theoretical revenue from collective bookings.                                                              |
| total_collective_real_revenue           |           | Total actual revenue from collective bookings.                                                                   |
| total_theoretic_revenue                 |           | Total theoretical revenue from all bookings.                                                                     |
| total_real_revenue                      |           | Total actual revenue from all bookings.                                                                          |
| total_current_year_real_revenue         |           | Total real revenue for the current year.                                                                         |
| first_collective_booking_date           |           | Date of the first collective booking.                                                                            |
| last_collective_booking_date            |           | Date of the last collective booking.                                                                             |
| total_created_individual_offers         |           | Total number of individual offers created.                                                                       |
| total_created_collective_offers         |           | Total number of collective offers created.                                                                       |
| total_created_offers                    |           | Total number of offers created, both individual and collective.                                                  |
| total_bookable_individual_offers        |           | Total number of individual offers that are bookable.                                                             |
| total_bookable_collective_offers        |           | Total number of collective offers that are bookable.                                                             |
| total_bookable_offers                   |           | Total number of offers that are bookable, both individual and collective.                                        |
| offerer_siren                           |           | SIREN number of the offerer.                                                                                     |
| legal_unit_business_activity_code       |           | Business activity code of the legal unit of the offerer.                                                         |
| legal_unit_business_activity_label      |           | Business activity label of the legal unit of the offerer.                                                        |
| legal_unit_legal_category_code          |           | Legal category code of the legal unit of the offerer.                                                            |
| legal_unit_legal_category_label         |           | Legal category label of the legal unit of the offerer.                                                           |
| is_local_authority                      |           | Indicates if the offerer is a local authority or not.                                                            |
| total_managed_venues                    |           | Total number of venues managed by the offerer.                                                                   |
| total_permanent_managed_venues          |           | Total number of permanent venues managed by the offerer.                                                         |
| total_venues                            |           | Total number of venues associated with the offerer.                                                              |
| offerer_humanized_id                    |           | Unique identifier of the offerer used on Flaskadmin and Matomo.                                                  |
| first_dms_adage_status                  |           | First DMS adage status of the offerer.                                                                           |
| dms_submitted_at                        |           | Date when the offerer sumitted his first DMS record.                                                             |
| dms_accepted_at                         |           | Date when the offerer was accepted in DMS.                                                                       |
| is_reference_adage                      |           | Indicates if the offerer is a reference in adage.                                                                |
| is_synchro_adage                        |           | Indicates if the offerer is synchronized with adage.                                                             |
| local_authority_type                    |           | When the offerer is a local authority, the type of the local authority, such as Commune, Département, or Région. |
| local_authority_is_priority             |           | Indicates whether the local authority is internally seen as a priority partner.                                  |
| offerer_is_epn                          | BOOLEAN   | Boolean true when the offerer is flagged as a national public structure.                                         |
