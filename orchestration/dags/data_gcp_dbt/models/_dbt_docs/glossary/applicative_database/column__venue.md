{% docs column__venue_id %} Unique identifier for the venue. {% enddocs %}
{% docs column__venue_name %} Name of the venue. {% enddocs %}
{% docs column__venue_public_name %} Name of the venue displayed on the application. Can be different of the venue_name if the partner want display a more descriptive name for example. {% enddocs %}
{% docs column__venue_booking_email %} Email displayed on the application, can be used to book the offer or reach to the partner. {% enddocs %}
{% docs column__venue_street %} Street address of the venue. {% enddocs %}
{% docs column__venue_latitude %} Latitude coordinate of the venue. {% enddocs %}
{% docs column__venue_longitude %} Longitude coordinate of the venue. {% enddocs %}
{% docs column__venue_department_code %} Department code of the venue. {% enddocs %}
{% docs column__venue_postal_code %} Postal code of the venue. {% enddocs %}
{% docs column__venue_city %} City where the venue is located. {% enddocs %}
{% docs column__venue_city_code %} City code where the venue is located. {% enddocs %}
{% docs column__venue_siret %} SIRET number of the venue. A venue may not have a SIRET (especially when it hosts public but does not belong to the offerer. Exemple : a theater company performs a play in a concert hall they don't own.) {% enddocs %}
{% docs column__venue_is_virtual %} Indicates if the venue is virtual. Each offerer has a virtual venue, it is only used to publish digital offers. NB : digital venues will soon be deleted, their offers/bookings/stocks will be transferred to other venues of the offerer. {% enddocs %}
{% docs column__venue_managing_offerer_id %} ID of the offerer who manage the venue. One offerer can have multiple venues. {% enddocs %}
{% docs column__venue_creation_date %} Date when the venue was created on the application. {% enddocs %}
{% docs column__venue_is_permanent %} Indicates if the venue is permanent. A permanent venue is a venue that can receive public permanently, that can propose offers, and that is managed by the partner. Permanent venues exemple : a library, a cinema. Non permanent venues example : a public garden that hosted a festival once, or a theater that hosted a concert once - it can be permanent for the partner who owns the theater, but not for the partner who is hosted once in this place. {% enddocs %}
{% docs column__venue_is_open_to_public %} This field will replace the venue_is_permanent field (mid-2025), as part of the offer-adresse project. It is a venue that can receive public permanently, that can propose offers, and that is managed by the partner. {% enddocs %}
{% docs column__venue_is_acessibility_synched %} Indicates if the venue's accessibility is synchronized. {% enddocs %}
{% docs column__venue_type_label %} Type of the venue ('Musée', 'Cinéma','Librairie', etc). Selected by the partner in a drop-down list during subscription. If you need to study bookings and offers per cultural sector, use the offer categories instead. "Autre" category contains many administrative venues of live performance actors and local authorities. NB : this venue_type_label segmentation is limited and currently being revised.  {% enddocs %}
{% docs column__venue_label %} Label of the venue. The label is appended by the Ministry as token of quality and standing in its category. {% enddocs %}
{% docs column__venue_humanized_id %} Unique identifier of the venue. {% enddocs %}
{% docs column__venue_backoffice_link %} Backoffice link for the venue. {% enddocs %}
{% docs column__venue_region_name %} Region name where the venue is located. {% enddocs %}
{% docs column__venue_epci %} EPCI name of the venue. {% enddocs %}
{% docs column__venue_epci_code %} EPCI code of the venue. {% enddocs %}
{% docs column__venue_density_label %} Detailed density label of the venue's location. {% enddocs %}
{% docs column__venue_macro_density_label %} Macro density label of the venue's location (urbain, rural) {% enddocs %}
{% docs column__venue_academy_name %} Academy of the venue. {% enddocs %}
{% docs column__venue_targeted_audience %} Targeted audience for the venue : individual, educational, both.  {% enddocs %}
{% docs column__venue_description %} Description of the venue written by the partner. {% enddocs %}
{% docs column__venue_withdrawal_details %} Facultative description of the withdrawal in the venue. {% enddocs %}
{% docs column__venue_contact_phone_number %} Contact phone number of the venue displayed on the application (venue page). {% enddocs %}
{% docs column__venue_contact_email %} Contact email for the venue displayed on the application (venue page). {% enddocs %}
{% docs column__venue_contact_website %} Contact website for the venue displayed on the application. {% enddocs %}
{% docs column__venue_pc_pro_link %} PC Pro link for the venue. {% enddocs %}
{% docs column__venue_iris_internal_id %} Internal IRIS identifier for the venue. {% enddocs %}
{% docs column__venue_density_level %} ID of the density level of the venue (cf venue_density_label). {% enddocs %}
{% docs column__venue_department_name %} Department name where of the venue. {% enddocs %}
{% docs column__venue_has_siret %}Indicates whether the venue has a SIRET.{% enddocs %}
{% docs column__venue_in_qpv %}Indicates whether the venue is in a City Policy Priority Neighborhood.{% enddocs %}
{% docs column__venue_in_zrr %}Indicates whether the venue is in a ZRR.{% enddocs %}
{% docs column__venue_rural_city_type %}Type of rural city of the venue.{% enddocs %}
{% docs column__venue_seniority %}Seniority of the venue in days.{% enddocs %}
{% docs column__venue_image_source %}Origin of venue image : google, offerer, default_category.{% enddocs %}
{% docs column__venue_adage_inscription_date %}Date when the venue was synchonized on Adage (able to publish collective offers) .{% enddocs %}
{% docs column__total_distinct_headline_offers %}Number of distinct offers which were headlined on the app venue page.{% enddocs %}
{% docs column__has_headline_offer %}Indicates if an offer is currently headlined on the app venue page.{% enddocs %}
{% docs column__first_headline_offer_date %}First date of headline offer on the app venue page.{% enddocs %}
{% docs column__last_headline_offer_date %}Last date of headline offer on the app venue page.{% enddocs %}

/* To rename into venue_*** */
{% docs column__is_active_last_30days %} Analytical field: Indicates if it was active in the last 30 days. {% enddocs %}
{% docs column__is_active_current_year %} Analytical field: Indicates if it is active in the current year. {% enddocs %}
{% docs column__is_individual_active_last_30days %} Analytical field: Indicates if it had individual activity in the last 30 days. {% enddocs %}
{% docs column__is_individual_active_current_year %} Analytical field: Indicates if it has individual activity in the current year. {% enddocs %}
{% docs column__is_collective_active_last_30days %} Analytical field: Indicates if it had collective activity in the last 30 days. {% enddocs %}
{% docs column__is_collective_active_current_year %} Analytical field: Indicates if it has collective activity in the current year. {% enddocs %}
{% docs column__banner_url %}Venue image url.{% enddocs %}
