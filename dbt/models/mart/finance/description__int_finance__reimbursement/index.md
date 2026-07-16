The `int_finance__reimbursement` table computes finance amounts by booking_used_date like the reimbursement amount and the contribution amount. It is aggregated by category, departement and region.

## Table description

| name                      | data_type | description                                                                                                                     |
| ------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| booking_used_date         |           | Date when the booking was used.                                                                                                 |
| venue_department_code     |           | Department code of the venue.                                                                                                   |
| venue_department_name     |           | Department name where of the venue.                                                                                             |
| venue_region_name         |           | Region name where the venue is located.                                                                                         |
| venue_epci_code           |           | EPCI code of the venue.                                                                                                         |
| venue_city_code           |           | City code where the venue is located.                                                                                           |
| offerer_is_epn            |           | Boolean true when the offerer is flagged as a national public structure.                                                        |
| offer_category_id         |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu. |
| total_bookings            |           | Total number of bookings, both individual and collective.                                                                       |
| total_quantities          |           | Total quantities booked. A booking quantity can be 1 or 2.                                                                      |
| total_revenue_amount      |           | The total revenue amount associated with the bookings.                                                                          |
| total_reimbursed_amount   |           | Total amount reimbursed to the offerers.                                                                                        |
| total_contribution_amount |           | Total amount contributed by the offerers on their bookings.                                                                     |
