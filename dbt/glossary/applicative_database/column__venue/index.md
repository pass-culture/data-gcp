**venue_id**: Unique identifier for the venue.

**venue_name**: Name of the venue.

**venue_public_name**: Name of the venue displayed on the application. Can be different of the venue_name if the partner want display a more descriptive name for example.

**venue_booking_email**: Email displayed on the application, can be used to book the offer or reach to the partner.

**venue_street**: Street address of the venue.

**venue_latitude**: Latitude coordinate of the venue.

**venue_longitude**: Longitude coordinate of the venue.

**venue_department_code**: Department code of the venue.

**venue_postal_code**: Postal code of the venue.

**venue_city**: City where the venue is located.

**venue_city_code**: City code where the venue is located.

**venue_siret**: SIRET number of the venue. A venue may not have a SIRET (especially when it hosts public but does not belong to the offerer. Exemple : a theater company performs a play in a concert hall they don't own.)

**venue_managing_offerer_id**: ID of the offerer who manage the venue. One offerer can have multiple venues.

**venue_creation_date**: Date when the venue was created on the application.

**venue_is_permanent**: Indicates if the venue is permanent. A permanent venue is a venue that can receive public permanently, that can propose offers, and that is managed by the partner. Permanent venues exemple : a library, a cinema. Non permanent venues example : a public garden that hosted a festival once, or a theater that hosted a concert once - it can be permanent for the partner who owns the theater, but not for the partner who is hosted once in this place.

**venue_is_open_to_public**: This field will replace the venue_is_permanent field (mid-2025), as part of the offer-adresse project. It is a venue that can receive public permanently, that can propose offers, and that is managed by the partner.

**venue_is_acessibility_synched**: Indicates if the venue's accessibility is synchronized.

**venue_type_label**: Type of the venue ('Musée', 'Cinéma','Librairie', etc). Selected by the partner in a drop-down list during subscription. If you need to study bookings and offers per cultural sector, use the offer categories instead. "Autre" category contains many administrative venues of live performance actors and local authorities. NB : this venue_type_label segmentation is limited and currently being revised.

**venue_activity**: V2 Type of the venue ('Musée', 'Cinéma','Librairie', etc). Selected by the partner in a drop-down list during subscription. If you need to study bookings and offers per cultural sector, use the offer categories instead. NB : this is the new version of venue_type_label segmentation which is currently being revised. This field enables dual-writing during the data backfill phase.

**venue_label**: Label of the venue. The label is appended by the Ministry as token of quality and standing in its category.

**venue_humanized_id**: Unique identifier of the venue.

**venue_backoffice_link**: Backoffice link for the venue.

**venue_region_name**: Region name where the venue is located.

**venue_epci**: EPCI name of the venue.

**venue_epci_code**: EPCI code of the venue.

**venue_density_label**: Detailed density label of the venue's location.

**venue_macro_density_label**: Macro density label of the venue's location (urbain, rural)

**venue_academy_name**: Academy of the venue.

**venue_targeted_audience**: Targeted audience for the venue : individual, educational, both.

**venue_description**: Description of the venue written by the partner.

**venue_withdrawal_details**: Facultative description of the withdrawal in the venue.

**venue_contact_phone_number**: Contact phone number of the venue displayed on the application (venue page).

**venue_contact_email**: Contact email for the venue displayed on the application (venue page).

**venue_contact_website**: Contact website for the venue displayed on the application.

**venue_pc_pro_link**: PC Pro link for the venue.

**venue_iris_internal_id**: Internal IRIS identifier for the venue.

**venue_density_level**: ID of the density level of the venue (cf venue_density_label).

**venue_department_name**: Department name where of the venue.

**venue_has_siret**: Indicates whether the venue has a SIRET.

**venue_in_qpv**: Indicates whether the venue is in a City Policy Priority Neighborhood.

**venue_in_zrr**: Indicates whether the venue is in a ZRR.

**venue_rural_city_type**: Type of rural city of the venue.

**venue_seniority**: Seniority of the venue in days.

**venue_image_source**: Origin of venue image : google, offerer, default_category.

**venue_adage_inscription_date**: Date when the venue was synchonized on Adage (able to publish collective offers) .

**total_distinct_headline_offers**: Number of distinct offers which were headlined on the app venue page.

**has_headline_offer**: Indicates if an offer is currently headlined on the app venue page.

**first_headline_offer_date**: First date of headline offer on the app venue page.

**last_headline_offer_date**: Last date of headline offer on the app venue page.

/ *To rename into venue\_*\*/

**is_active_last_30days**: Analytical field: Indicates if it was active in the last 30 days.

**is_active_current_year**: Analytical field: Indicates if it is active in the current year.

**is_individual_active_last_30days**: Analytical field: Indicates if it had individual activity in the last 30 days.

**is_individual_active_current_year**: Analytical field: Indicates if it has individual activity in the current year.

**is_collective_active_last_30days**: Analytical field: Indicates if it had collective activity in the last 30 days.

**is_collective_active_current_year**: Analytical field: Indicates if it has collective activity in the current year.

**banner_url**: Venue image url.

**venue_volunteering_url**: URL where users can find volunteering information for the venue.
