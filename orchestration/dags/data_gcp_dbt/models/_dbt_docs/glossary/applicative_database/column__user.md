{% docs column__user_academy_name %} Academy name associated with the user's registered address. {% enddocs %}
{% docs column__user_activated_at %} Date when the user's account was activated. Corresponds to the first booking date for users in experiment phase, and to user creation date for all users after the experiment phase. {% enddocs %}
{% docs column__user_activity %} User's registered activity (student, apprentice, unemployed etc). Registered at first grant deposit and updated when the user applies for its GRANT_18. {% enddocs %}
{% docs column__user_age %} Current age of the user. {% enddocs %}
{% docs column__user_birth_date %} Birth date of the user. {% enddocs %}
{% docs column__user_civility %} Registered civility of the user (male, female). Information collected during registration.{% enddocs %}
{% docs column__user_city %} City associated with the user's registered address. {% enddocs %}
{% docs column__user_city_code %} The INSEE code of the city associated with the user's registered address. {% enddocs %}
{% docs column__user_created_at %} Date when the user account was created. {% enddocs %}
{% docs column__user_department_code %} Department code associated with the user's registered address. {% enddocs %}
{% docs column__user_department_name %} Department name associated with the user's registered address. {% enddocs %}
{% docs column__user_density_label %} String column.Density label (urban, rural) of the user's registered address. {% enddocs %}
{% docs column__user_density_level %} Integer column. Density level of the user's registered address. Ranges from 1 (highly urban) to 7 (highly rural). {% enddocs %}
{% docs column__user_epci %} EPCI code associated with the user's registered address. An EPCI is a French public body enabling municipalities to collaborate on shared local services and development. {% enddocs %}
{% docs column__user_humanized_id %} Human-readable identifier for the user. {% enddocs %}
{% docs column__user_id %} Unique identifier for a user. {% enddocs %}
{% docs column__user_iris_internal_id %} Internal IRIS identifier associated with the user's registered address. IRIS (Ilots Regroupés pour l'Information Statistique) are small, standardized geographic units used for detailed statistical analysis in France. {% enddocs %}
{% docs column__user_is_active %} Boolean. Indicates if the user's account is currently active (the user can access it, irrespective of grant status). {% enddocs %}
{% docs column__user_is_current_beneficiary %} Boolean. Indicates if the user still has available grant to use. {% enddocs %}
{% docs column__user_is_in_education %} Boolean. Indicates if the user is in education, based on their registered activity. According to the INSEE, a user is considered to be in education if they fall under one of the following categories: Middle school student (Collégien), High school student (Lycéen), University student (Étudiant), Apprentice (Apprenti), Work-study student (Alternant). {% enddocs %}
{% docs column__user_is_in_qpv %} Boolean. Indicates if the user's registered address is in a priority neighborhood (QPV). {% enddocs %}
{% docs column__user_is_priority_public %} Boolean. Indicates if the user considered as a pass Culture priority public (users that are either residing in a rural area, in a QPV or are not in education). {% enddocs %}
{% docs column__user_is_unemployed %} Boolean. Indicates if the user is unemployed as per its registered activity. {% enddocs %}
{% docs column__user_macro_density_label %} Macro density label of the user's registered address. {% enddocs %}
{% docs column__user_postal_code %} Postal code of the user's registered address. {% enddocs %}
{% docs column__user_region_name %} Region name of the user's registered address. {% enddocs %}
{% docs column__user_school_type %} Type of school the user is enrolled in: Centre de formation apprentis, Collège privé, Collège public, Lycée agricole, Lycée maritime, Lycée militaire, Lycée privé, Lycée public, À domicile (CNED, institut de santé, etc.). {% enddocs %}
{% docs column__user_seniority %} Days between user account creation date and current date. {% enddocs %}
{% docs column__user_suspension_reason %} Reason for the user's suspension. Possible values include: end of eligibility – The user no longer meets the criteria to be eligible for a deposit; fraud suspicion – Suspicious activity detected, requiring investigation; hacking fraud – Confirmed or suspected account compromise; upon user request – The user has explicitly requested the suspension or deletion of their account. {% enddocs %}
{% docs column__user_has_enabled_marketing_email %} Indicates if the user has accepted to receive marketing emails. {% enddocs %}
{% docs column__user_has_enabled_marketing_push %} Indicates if the user has accepted to received marketing push. {% enddocs %}
{% docs column__user_role %} Role assigned to the user (GRANT_18, GRANT_15_17, PRO, ADMIN). {% enddocs %}
{% docs column__user_address %} User's registered address. Registered at first grant deposit and updated when the user applies for its GRANT_18. {% enddocs %}
{% docs column__user_last_connection_date %} Date of the user's last connection. {% enddocs %}
{% docs column__user_is_email_validated %} Boolean. Indicates if the user's email is validated. {% enddocs %}
{% docs column__user_has_seen_pro_tutorials %} Boolean. Indicates if the user has seen professional tutorials. {% enddocs %}
{% docs column__user_phone_validation_status %} Status of the user's phone validation step. {% enddocs %}
{% docs column__user_has_validated_email %} Indicates if the user has validated their email. {% enddocs %}
{% docs column__user_currently_subscribed_themes %} Users themes subscribed. Users can subscribe to themes (cinema, music) to receive custom communication related to those themes. {% enddocs %}
{% docs column__user_is_theme_subscribed %} Boolean. Indicates whether a user has subscribed to at least one theme. {% enddocs %}
{% docs column__user_last_deposit_expiration_date %} Expiration date of the user's last deposit. {% enddocs %}
{% docs column__user_last_deposit_amount %} Amount of the last deposit received by the user. {% enddocs %}
{% docs column__user_first_deposit_type %} Type of the user's first deposit, can be GRANT_18, GRANT_15_17, GRANT_17_18, GRANT_FREE. {% enddocs %}
{% docs column__user_current_deposit_type %} Type of the user's current deposit. {% enddocs %}
{% docs column__user_first_deposit_reform_category %} The first deposit reform category associated with the user. {% enddocs %}
{% docs column__user_current_deposit_reform_category %} The current deposit reform category associated with the user. {% enddocs %}
{% docs column__user_expiration_month %} Month of the user's credit expiration {% enddocs %}
{% docs column__days_between_activation_date_and_first_booking_date %} Number of days between the user's activation date and their first booking date. {% enddocs %}
{% docs column__days_between_activation_date_and_first_booking_paid %} Number of days between the user's activation date and their first paid booking. {% enddocs %}
{% docs column__user_first_booking_type %} Offer category of the user's first booking. {% enddocs %}
{% docs column__user_first_paid_booking_type %} Offer category of the user's first paid booking. {% enddocs %}
{% docs column__user_first_deposit_amount %} Amount of the user's first deposit received. {% enddocs %}
{% docs column__user_has_added_offer_to_favorites %} Boolean. Indicates if the user has added any offer to their favorites. {% enddocs %}
{% docs column__user_qpi_subcategories %} QPI stands for 'Initial Practice Questionnaires'. We asked young users about their cultural practices before using the Pass, resulting in a list of subcategories used during the cold start to display offers based on these initial practices. {% enddocs %}
{% docs column__user_modified_at %} Timestamp at which user has updated its informations. {% enddocs %}
{% docs column__user_age_at_booking %} The age of the user at the time of booking, calculated as the difference between the booking date and the user's date of birth. {% enddocs %}
{% docs column__user_age_at_deposit %} The age of the user at the time of deposit, calculated as the difference between the deposit date and the user's date of birth. {% enddocs %}
{% docs column__user_age_at_first_deposit %} The age of the user at the time of the first deposit on their account, calculated as the difference between the deposit date and the user's date of birth. {% enddocs %}
{% docs column__user_age_at_last_deposit %} The age of the user at the time of the last deposit made on their account, calculated as the difference between the deposit date and the user's date of birth. {% enddocs %}
{% docs column__user_address_geocode_type %} Geocode precision type (street, municipality, etc) of the user's registered address. {% enddocs %}
{% docs column__user_address_latitude %} Latitude of the user's registered address. {% enddocs %}
{% docs column__user_address_longitude %} Longitude of the user's registered address. {% enddocs %}
{% docs column__user_address_raw %} Raw address of the user's registered address. {% enddocs %}
{% docs column__user_category %} User category derived from the user's role and age. Possible values include: '15-16' (users aged 15 or 16 with no specific role), 'general_public' (users aged 14 or under, or 17 and above, with no specific role), or 'beneficiary' (for users with a non-null role). {% enddocs %}
{% docs column__user_action_type %} Type of action that triggered the user information record. Possible values: INFO_MODIFIED (user modified their profile), PROFILE_COMPLETION (initial profile completion during registration). {% enddocs %}
{% docs column__user_previous_activity %} User's activity status before the current change. {% enddocs %}
{% docs column__user_age_at_info_creation %} User's age in years at the time the information record was created. {% enddocs %}
{% docs column__user_qpv_code %} Code of the QPV (Quartier Prioritaire de la Ville) if the user is located in a priority neighborhood. {% enddocs %}
{% docs column__user_qpv_name %} Name of the QPV (Quartier Prioritaire de la Ville) if the user is located in a priority neighborhood. {% enddocs %}
{% docs column__user_epci_code %} Code of the EPCI (Etablissement Public de Cooperation Intercommunale) where the user is located. {% enddocs %}
{% docs column__user_density_macro_level %} Macro-level urban density classification (Urban vs Rural). Aggregation of density_label into broader categories. {% enddocs %}
{% docs column__info_history_rank %} Sequential rank of information changes for each user (0 = first record, 1 = second, etc.). {% enddocs %}
{% docs column__has_confirmed %} Boolean flag indicating if the user confirmed their existing information (all fields remained the same compared to previous record). {% enddocs %}
{% docs column__has_modified %} Boolean flag indicating if the user modified any of their information (at least one field changed compared to previous record). {% enddocs %}
{% docs column__has_modified_activity %} Boolean flag indicating if user_activity changed compared to previous record. {% enddocs %}
{% docs column__has_modified_address %} Boolean flag indicating if user address changed compared to previous record (normalized comparison). {% enddocs %}
{% docs column__has_modified_city %} Boolean flag indicating if user city changed compared to previous record. {% enddocs %}
{% docs column__has_modified_postal_code %} Boolean flag indicating if user postal code changed compared to previous record. {% enddocs %}
