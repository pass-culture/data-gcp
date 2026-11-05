---
description: Description of the `metric` columns.
title: Metrics
---

{% docs column__total_actual_amount_spent %} Amount of the used individual bookings (booking_is_used) on the row's deposits, in euros (all of the user's deposits on a user row): spending counted at use time, bookings not yet used excluded. {% enddocs %}
{% docs column__total_available_stock %} Quantity of remaining stock. (stock_quantity - non cancelled bookings){% enddocs %}
{% docs column__total_bookable_collective_offers %} Total number of collective offers that are bookable. {% enddocs %}
{% docs column__total_bookable_individual_offers %} Total number of individual offers that are bookable. {% enddocs %}
{% docs column__total_bookable_offers %} Total number of offers that are bookable, both individual and collective. {% enddocs %}
{% docs column__total_bookings %} Total number of bookings, both individual and collective. {% enddocs %}
{% docs column__finance_individual__total_bookings %} Number of distinct used individual bookings having an invoiced pricing in a payment batch, for the month of use and the row's cell. {% enddocs %}
{% docs column__total_cancelled_bookings %}Total number of cancelled bookings.{% enddocs %}
{% docs column__total_cancelled_individual_bookings %} Total number of cancelled individual bookings. {% enddocs %}
{% docs column__total_collective_bookings %} Number of collective bookings created in the month that are confirmed (confirmed, used, pending reimbursement or reimbursed). A booking created before its scholar year starts is counted in the first month of that scholar year. {% enddocs %}
{% docs column__total_collective_amount_spent %} Total amount spent for collective bookings. {% enddocs %}
{% docs column__cumulative_total_collective_bookings %}
Cumulative total number of collective bookings for the same geography from the start of the given school year up to and including the specified month.
{% enddocs %}

{% docs column__cumulative_total_collective_amount_spent %}
Cumulative total amount spent for collective bookings for the same geography from the start of the given school year up to and including the specified month.
{% enddocs %}
{% docs column__total_prereservations %} Number of non-cancelled collective bookings (pre-reservations, whatever their status) created in the month. A booking created before its scholar year starts is counted in the first month of that scholar year. {% enddocs %}
{% docs column__total_prereservation_amount %} Amount of the non-cancelled collective bookings created in the month (same attribution as `total_prereservations`). {% enddocs %}
{% docs column__total_confirmations %} Number of collective bookings confirmed by the school head in the month (by confirmation date, clamped into the booking's scholar year). {% enddocs %}
{% docs column__total_confirmation_amount %} Amount of the collective bookings confirmed in the month. {% enddocs %}
{% docs column__total_actions %} Number of confirmed collective bookings whose action (stock beginning date) takes place in the month. Future months give a forecast. {% enddocs %}
{% docs column__total_action_amount %} Amount of the confirmed collective bookings whose action takes place in the month. {% enddocs %}
{% docs column__cumulative_total_prereservations %} `total_prereservations` summed from the start of the scholar year up to and including the month. {% enddocs %}
{% docs column__cumulative_total_prereservation_amount %} `total_prereservation_amount` summed from the start of the scholar year up to and including the month. {% enddocs %}
{% docs column__cumulative_total_confirmations %} `total_confirmations` summed from the start of the scholar year up to and including the month. {% enddocs %}
{% docs column__cumulative_total_confirmation_amount %} `total_confirmation_amount` summed from the start of the scholar year up to and including the month. {% enddocs %}
{% docs column__cumulative_total_actions %} `total_actions` summed from the start of the scholar year up to and including the month. {% enddocs %}
{% docs column__cumulative_total_action_amount %} `total_action_amount` summed from the start of the scholar year up to and including the month. {% enddocs %}
{% docs column__total_created_bookable_collective_offers %} Number of bookable (non template) collective offers created in the month. An offer belongs to the scholar year of its action (stock beginning date); an offer created before that scholar year starts is counted in its first month. {% enddocs %}
{% docs column__total_prereserved_bookable_collective_offers %} Number of the bookable collective offers created in the month that have at least one non-cancelled booking. {% enddocs %}
{% docs column__new_bookable_offer_partners %} Number of cultural partners (venues) whose first bookable collective offer of the scholar year is created in the month. Summed over months, it gives the number of distinct partners. {% enddocs %}
{% docs column__cumulative_total_created_bookable_collective_offers %} `total_created_bookable_collective_offers` summed from the start of the scholar year up to and including the month. {% enddocs %}
{% docs column__cumulative_total_prereserved_bookable_collective_offers %} `total_prereserved_bookable_collective_offers` summed from the start of the scholar year up to and including the month. {% enddocs %}
{% docs column__cumulative_new_bookable_offer_partners %} Number of distinct cultural partners (venues) with at least one bookable collective offer created since the start of the scholar year, up to and including the month. {% enddocs %}
{% docs column__total_collective_offers %}The total number of collective offers.{% enddocs %}
{% docs column__total_collective_real_revenue %} Total actual revenue from collective bookings. {% enddocs %}
{% docs column__total_collective_theoretic_revenue %} Total theoretical revenue from collective bookings. {% enddocs %}
{% docs column__total_created_collective_offers %} Total number of collective offers created. {% enddocs %}
{% docs column__total_created_individual_offers %} Total number of individual offers created. {% enddocs %}
{% docs column__total_created_offers %} Total number of offers created, both individual and collective. {% enddocs %}
{% docs column__total_credited_beneficiaries %}The total number of credited beneficiaries.{% enddocs %}
{% docs column__total_current_year_collective_real_revenue %}The total real revenue from collective bookings for the current year.{% enddocs %}
{% docs column__total_current_year_non_cancelled_tickets %} Total number of tickets that were not cancelled in the current year. {% enddocs %}
{% docs column__total_current_year_real_revenue %} Total real revenue for the current year. {% enddocs %}
{% docs column__total_current_scholar_year_tickets %}The total number of tickets for the current scholar year.{% enddocs %}
{% docs column__total_current_year_non_cancelled_collective_bookings %} The total non cancelled collective bookings for the current year. {% enddocs %}
{% docs column__total_current_year_collective_theoretic_revenue %} Total collective theoretical revenue for the current year. {% enddocs %}
{% docs column__total_current_year_used_collective_bookings %} Total collective used bookings for the current year. {% enddocs %}

{% docs column__total_deposit_actual_amount_spent %} Amount of the used individual bookings on the user's last deposit only, in euros. {% enddocs %}
{% docs column__total_deposit_amount %} Total amount deposited by the user. {% enddocs %}
{% docs column__total_deposits %}The total number of deposits for the educational institution.{% enddocs %}
{% docs column__total_grant_18_subcategory_booked %} The total number of subcategory booked on grant_18 deposit. {% enddocs %}
{% docs column__total_grant_15_17_subcategory_booked %} The total number of subcategory booked on grant_15_17 deposit. {% enddocs %}
{% docs column__total_favorites %} Total number of times this offer was favorited. {% enddocs %}
{% docs column__total_first_bookings %} Total number of bookings that are first bookings. {% enddocs %}
{% docs column__total_free_bookings %} Total number of free bookings made by the user. {% enddocs %}
{% docs column__total_individual_bookings %} Total number of individual bookings. {% enddocs %}
{% docs column__total_individual_offers %}The total number of individual offers.{% enddocs %}
{% docs column__total_individual_real_revenue %} Total actual revenue from individual bookings. {% enddocs %}
{% docs column__total_individual_theoretic_revenue %} Total theoretical revenue from individual bookings. {% enddocs %}
{% docs column__total_last_deposit_digital_goods_amount_spent %} Total amount spent on digital goods from the last deposit. {% enddocs %}
{% docs column__total_last_deposit_theoretical_amount_spent %} Total theoretical amount spent from the last deposit. {% enddocs %}
{% docs column__total_non_cancelled_bookings %} Total number of bookings that were not cancelled, both individual and collective. {% enddocs %}
{% docs column__total_non_cancelled_collective_bookings %} Total number of collective bookings that were not cancelled. {% enddocs %}
{% docs column__total_non_cancelled_duo_bookings %} Total number of non-cancelled duo bookings made by the user. {% enddocs %}
{% docs column__total_non_cancelled_individual_bookings %} Total number of non-cancelled individual bookings made by the user. {% enddocs %}
{% docs column__total_non_cancelled_tickets %} Total number of tickets that were not cancelled. {% enddocs %}
{% docs column__total_paid_bookings %}Total number of paid bookings.{% enddocs %}
{% docs column__total_permanent_managed_venues %}The total number of permanent venues managed by the local authority.{% enddocs %}
{% docs column__total_real_revenue %} Total actual revenue from all bookings. {% enddocs %}
{% docs column__total_reimbursement_points %}The total number of reimbursement points for the local authority.{% enddocs %}
{% docs column__total_stock_quantity %} Total remaining stock quantity. {% enddocs %}
{% docs column__total_students %}The total number of students for the educational institution.{% enddocs %}
{% docs column__total_eligible_students %}
Total number of students eligible for EAC (Education Artistique et Culturelle) within the territory for the specified month and school year.
{% enddocs %}

{% docs column__total_engaged_students %}
Total number of students who participated in at least one EAC activity within the territory for the specified month and school year.
{% enddocs %}
{% docs column__total_theoretic_revenue %} Total theoretical revenue from all bookings. {% enddocs %}
{% docs column__total_theoretical_amount_spent %} Amount of the non-cancelled individual bookings (booking_intermediary_amount), used or not, on the row's deposits, in euros (all of the user's deposits on a user row): spending counted at booking time. {% enddocs %}
{% docs column__total_theoretical_amount_spent_in_digital_goods %} Total theoretical amount spent on digital goods based on expected transactions. {% enddocs %}
{% docs column__total_theoretical_digital_goods_amount_spent %} Total theoretical amount spent on digital goods by the user. {% enddocs %}
{% docs column__total_theoretical_outings_amount_spent %} Total theoretical amount spent on outings by the user. {% enddocs %}
{% docs column__total_theoretical_physical_goods_amount_spent %} Total theoretical amount spent on physical goods by the user. {% enddocs %}
{% docs column__total_theoretical_remaining_credit %} Credit left on the user's last deposit, in euros: last_deposit_amount minus the non-cancelled bookings on that deposit (total_last_deposit_theoretical_amount_spent). Earlier deposits do not enter it. It is not set to zero when the deposit has expired. {% enddocs %}
{% docs column__total_tickets %}The total number of tickets for the educational institution.{% enddocs %}
{% docs column__total_used_bookings %} Total number of bookings that were used. {% enddocs %}
{% docs column__total_used_collective_bookings %} Total number of collective bookings that were used. {% enddocs %}
{% docs column__total_used_individual_bookings %} Total number of used individual bookings. {% enddocs %}
{% docs column__total_users %} The total number of users. {% enddocs %}
{% docs column__monthly_beneficiary_coverage__total_users %} Number of distinct users with an active deposit (any deposit type) during the month, for the row's birth month and department (decimal-age grain). Only the last 48 months are computed. {% enddocs %}
{% docs column__total_users_last_12_months %} Rolling sum of distinct users over the last 12 months for a given age and department: the sum of monthly distinct-user counts over the current and 11 previous months, so a user active in several months is counted once per month. {% enddocs %}
{% docs column__total_population %} The total number of french population. {% enddocs %}
{% docs column__monthly_beneficiary_coverage__total_population %} INSEE estimated population for the row's snapshot month, birth month and department (decimal-age grain). A population count, not a count of registered users. {% enddocs %}
{% docs column__total_population_last_12_months %} Rolling sum of french population over the last 12 months for a given age and department. {% enddocs %}
{% docs column__total_3_category_booked_users %} The total number of users who made bookings in three categories or more. {% enddocs %}
{% docs column__total_amount_spent %} The total amount of credit spent by the user. {% enddocs %}
{% docs column__total_item_consulted %} The total number of items consulted by the user. {% enddocs %}
{% docs column__total_venue_consulted %} The total number of venues consulted by the user. {% enddocs %}
{% docs column__total_venue_type_label_consulted %} The total number of venues type label consulted by the user. {% enddocs %}
{% docs column__total_day_between_deposit_and_first_booking %} The total number of days elapsed between the user deposit creation date and the user first booking date. {% enddocs %}
{% docs column__total_diversification_score %} The total diversification score of the users. {% enddocs %}
{% docs column__total_venue_id_booked %} The total number of distinct venues booked by the users. Virtual venues are excluded. {% enddocs %}
{% docs column__total_venue_type_booked %} The total number of distinct venue type booked by the users. {% enddocs %}
{% docs column__total_category_booked %} The total number of distinct categories booked by the users. {% enddocs %}
{% docs column__ratio_current_credit_utilization %} The ratio of current credit utilization for the educational institution based on validated bookings {% enddocs %}
{% docs column__ratio_beneficiary_students %}The ratio of beneficiary students for the educational institution.{% enddocs %}
{% docs column__mean_stock_price_per_item %} The average stock price for the item. {% enddocs %}
{% docs column__total_diversity_score %} The total diversity score of the user or the deposit {% enddocs %}
{% docs column__total_venue_20_km %} The total distinct number of venues located under a 20 kilometers area around the centroid of the IRIS. {% enddocs %}
{% docs column__total_venue_5_km %} The total distinct number of venues located under a 5 kilometers area around the centroid of the IRIS. {% enddocs %}
{% docs column__total_population_15_years_or_more %} The total population over 15 years living in the IRIS. {% enddocs %}
{% docs column__total_population_11_17_years %} Population aged 11 to 17 living in the IRIS, from the 2020 INSEE census. {% enddocs %}
{% docs column__total_population_18_24_years %} Population aged 18 to 24 living in the IRIS, from the 2020 INSEE census; concentrated in university towns. {% enddocs %}
{% docs column__pct_csp_1 %} The percentage of the population, that have 15 years or more, belonging to the socio-professional category 1 - Farmers. {% enddocs %}
{% docs column__pct_csp_2 %} The percentage of the population, that have 15 years or more, belonging to the socio-professional category 2 - Craftsmen, Traders, Business Leaders. {% enddocs %}
{% docs column__pct_csp_3 %} The percentage of the population, that have 15 years or more, belonging to the socio-professional category 3 - Executives and Higher Intellectual Professions. {% enddocs %}
{% docs column__pct_csp_4 %} The percentage of the population, that have 15 years or more, belonging to the socio-professional category 4 - Intermediate professions. {% enddocs %}
{% docs column__pct_csp_5 %} The percentage of the population, that have 15 years or more, belonging to the socio-professional category 5 - Employees. {% enddocs %}
{% docs column__pct_csp_6 %} The percentage of the population, that have 15 years or more, belonging to the socio-professional category 6 - Workers. {% enddocs %}
{% docs column__pct_csp_7 %} The percentage of the population, that have 15 years or more, belonging to the socio-professional category 7 - Retired. {% enddocs %}
{% docs column__pct_csp_8 %} The percentage of the population, that have 15 years or more, belonging to the socio-professional category 8 - Others without professional activity. {% enddocs %}
{% docs column__pct_french_population %} The percentage of the population that have French nationality, all ages included. {% enddocs %}
{% docs column__pct_foreign_population %} The percentage of the population that have foreign nationality, all ages included. {% enddocs %}
{% docs column__total_session_display %} Number of sessions with a display of the row's object (module_id, offer_id...) that day. Sessions are counted per object: summing across objects counts a session once per object displayed. {% enddocs %}
{% docs column__total_session_with_click %} The total number of sessions with a click on a specific object like module_id or offer_id. {% enddocs %}
{% docs column__total_sesh_consult_offer %} The total number of sessions with a consultation related to a specific object like module_id or offer_id. {% enddocs %}
{% docs column__total_session_fav %} The total number of sessions which puts an offer to favorite, related to a specific object like module_id or offer_id. {% enddocs %}
{% docs column__total_session_with_consult_video %} The total number of sessions with a video consultation, related to a specific object like module_id. {% enddocs %}
{% docs column__total_click %} The total number of clicks on a specific object like module_id or offer_id. {% enddocs %}
{% docs column__total_visit_last_month %} Number of distinct app sessions by the user in the last 30 days, computed as count of distinct (user_pseudo_id, session_id) pairs from Firebase native events. {% enddocs %}
{% docs column__total_consult_offer %} The total number of offer consultations. {% enddocs %}
{% docs column__total_fav %} The total number of bookmark actions. {% enddocs %}
{% docs column__total_session_with_booking %} The total number of session which performed a booking. {% enddocs %}
{% docs column__total_venue_map_consult %} The total number of consultations from the venue map. {% enddocs %}
{% docs column__total_venue_map_preview %} The total number of previews of the venue map. {% enddocs %}
{% docs column__total_consult_venue %} The total number of venues consulted from the venue map. {% enddocs %}
{% docs column__total_distinct_venue_consult_offer %} The total distinct number of venues which led to an offer consultation. {% enddocs %}
{% docs column__total_session_venue_map_seen_duration_seconds %} Durée totale en seconde pendant laquelle la venue map a été affichée au cours d’une session. {% enddocs %}
{% docs column__total_diversification %} Total diversification score of the bookings attributed to the entry (sum of the bookings' diversity_score). {% enddocs %}
{% docs column__total_quantities %} Number of seats booked: the sum of booking quantities, where a duo booking counts 2. Not a number of bookings. {% enddocs %}
{% docs column__total_revenue_amount %} Revenue: sum of booking_intermediary_amount of used individual bookings that have an invoiced pricing included in a payment batch, at the row's grain. {% enddocs %}
{% docs column__total_reimbursed_amount %} Amount reimbursed by pass Culture to offerers at the row's grain: pricing amounts on 'offerer revenue' pricing lines of invoiced pricings in a payment batch. {% enddocs %}
{% docs column__total_contribution_amount %} Total amount contributed by the offerers on their bookings, i.e. the share of revenue borne by the offerers (revenue minus reimbursed amount): 'offerer contribution' pricing lines of invoiced pricings in a payment batch, at the row's grain. {% enddocs %}
{% docs column__total_booking_amount %} Total amount generated by the bookings.  {% enddocs %}
{% docs column__total_booking_quantity %} Total quantity of items booked.  {% enddocs %}
{% docs column__total_booking_amount_ranked %} The rank of the total amount generated by the bookings. As opposed to the other category booked.  {% enddocs %}
{% docs column__total_venue_booking_amount_ranked %} The rank of the venue, ordered by total amount generated by the bookings.  {% enddocs %}
{% docs column__total_number_of_tickets %} Total number of tickets booked relative to collective bookings.  {% enddocs %}

{% docs column__sept_dec_total_theoric_bookings %} Total non cancelled collective bookings made during sept-dec period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__jan_aug_total_theoric_bookings %} Total non cancelled collective bookings made during jan-aug period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__sept_dec_theoric_amount_spent %} Total amount associated to the number of non cancelled collective bookings made during sept-dec period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__jan_aug_theoric_amount_spent %} Total amount associated to the number of non cancelled collective bookings made during jan-aug period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__sept_dec_total_confirmed_bookings %} Total confirmed, used and reimbursed collective bookings made during sept-dec period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__jan_aug_total_confirmed_bookings %} Total confirmed, used and reimbursed collective bookings made during jan-aug period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__sept_dec_real_amount_spent %} Total amount associated to the number of used and reimbursed collective bookings made during sept-dec period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__jan_aug_real_amount_spent %} Total amount associated to the number of used and reimbursed collective bookings made during jan-aug period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__sept_dec_reimbursed_amount %} Total reimbursed amount during sept-dec period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__jan_aug_reimbursed_amount %} Total reimbursed amount during jan-aug period - only for the institutions and scholar year with a split deposit.  {% enddocs %}
{% docs column__total_scholar_year_deposit %} Total amount of the educational deposit for the scholar year (if the deposit is split it is the sum of sept-dec and jan-aug deposit).  {% enddocs %}
{% docs column__all_year_total_theoric_bookings %} Total non cancelled collective bookings made during scholar year (if the deposit is split it is the sum of sept-dec and jan-aug deposit).  {% enddocs %}
{% docs column__all_year_theoric_amount_spent %} Total amount associated to the number of non cancelled collective bookings made during scholar year (if the deposit is split it is the sum of sept-dec and jan-aug deposit).  {% enddocs %}
{% docs column__pct_all_year_theoric_amount_spent %} All year theoric amount spent / all year deposit.  {% enddocs %}
{% docs column__pct_sept_dec_theoric_amount_spent %} Sept-dec theoric amount spent / sept_dec deposit (only for the institutions and scholar year with a split deposit).  {% enddocs %}
{% docs column__pct_jan_aug_theoric_amount_spent %} Jan-aug theoric amount spent / jan-aug deposit (only for the institutions and scholar year with a split deposit).  {% enddocs %}
{% docs column__all_year_total_confirmed_bookings %} Total confirmed, used and reimbursed collective bookings made during scholar year (if the deposit is split it is the sum of sept-dec and jan-aug deposit).  {% enddocs %}
{% docs column__all_year_real_amount_spent %} Total amount associated to the number of used and reimbursed collective bookings made during scholar year (if the deposit is split it is the sum of sept-dec and jan-aug deposit).  {% enddocs %}
{% docs column__pct_sept_dec_real_amount_spent %} Sept-dec real amount spent / sept-dec deposit (only for the institutions and scholar year with a split deposit).  {% enddocs %}
{% docs column__pct_jan_aug_real_amount_spent %} Jan-aug real amount spent / jan-aug deposit (only for the institutions and scholar year with a split deposit).  {% enddocs %}
{% docs column__pct_all_year_real_amount_spent %} Total scholar real amount spent / total scholar_year deposit (if the deposit is split it is the sum of sept-dec and jan-aug deposit).  {% enddocs %}
{% docs column__all_year_reimbursed_amount %} Total reimbursed amount during scholar year (if the deposit is split it is the sum of sept-dec and jan-aug deposit).  {% enddocs %}
{% docs column__is_split_deposit %} Whether or not the scholar year has one or two deposit.s associated. {% enddocs %}
{% docs column__sept_dec_deposit %} Amount of sept-dec deposit - only for the institutions and scholar year with a split deposit. {% enddocs %}
{% docs column__jan_aug_deposit %} Amount of jan-aug deposit - only for the institutions and scholar year with a split deposit. {% enddocs %}
{% docs column__pct_sept_dec_reimbursed_amount_spent %} Sept-dec reimbursed amount / sept-dec deposit (only for the institutions and scholar year with a split deposit).  {% enddocs %}
{% docs column__pct_jan_aug_reimbursed_amount_spent %} Jan-aug reimbursed amount / jan-aug deposit (only for the institutions and scholar year with a split deposit).  {% enddocs %}
{% docs column__pct_all_year_reimbursed_amount_spent %} Scholar year reimbursed amount / scholar year deposit (if the deposit is split it is the sum of sept-dec and jan-aug deposit).  {% enddocs %}
{% docs column__total_created_mediation_individual_offers %} Total number of individual offers with mediation (cultural outreach). {% enddocs %}
{% docs column__has_mediation_offer %} Indicates whether the venue has at least one offer with mediation (cultural outreach). {% enddocs %}

{% docs column__metrics__total_actual_beneficiaries %}
The total count of active beneficiaries at the time of calculation. To be included, a user must have a non-expired credit and a remaining amount of credit available for booking.
{% enddocs %}

{% docs column__metrics__total_beneficiaries %}
Number of beneficiaries at the end of the partition month, broken down by `age_at_calculation`. Counts every user with an active account (or suspended upon user request) who has ever received credit, regardless of whether the credit is currently active, exhausted, or expired. A user's `age_at_calculation` is recomputed each month, so the same user can appear in different age buckets across partition_months. Not a true running total — users with deactivated accounts drop from the count.
{% enddocs %}

{% docs column__metrics__total_category_booked_beneficiaries %}
The number of beneficiaries who made at least one non-cancelled booking in the specific category by the time their credit expired.
{% enddocs %}

{% docs column__metrics__total_3plus_category_booked_beneficiaries %}
The number of beneficiaries who booked across at least three different categories by the time their credit expired. This indicator is used to the diversification of cultural practices at the end of the beneficiary lifecycle.
{% enddocs %}

{% docs column__metrics__total_nplus_category_booked_beneficiaries %}
The number of beneficiaries who booked across at least N distinct categories (non-cancelled bookings) by the time their credit expired, N being the number in the column name (`total_Nplus_category_booked_beneficiaries`). Counts are cumulative: a beneficiary with 3 categories is counted in 1plus, 2plus and 3plus.
{% enddocs %}

{% docs column__metrics__total_spent_share_beneficiaries %}
The number of beneficiaries whose credit expired during the month and who spent a share of all credits received within the bucket in the column name (`total_spent_share_<min>_<max>_beneficiaries`): `0`, `]0-25]`, `]25-50]`, `]50-75]`, `]75-90]`, `]90-100]` (%). The last bucket has no strict 100% so that beneficiaries with a few cents left are counted with those who spent everything. Defined as a share of the credits received, it stays comparable across credit amounts.
{% enddocs %}

{% docs column__metrics__total_distinct_venue_booked_at_expiration %}
The sum, over beneficiaries whose credit expired during the month, of the number of distinct venues (partners) booked (non-cancelled bookings). Divide by `total_expired_deposit_beneficiaries` to get the average number of distinct partners booked.
{% enddocs %}

{% docs column__metrics__total_expired_deposit_beneficiaries %}
Number of beneficiaries whose GRANT_18 or GRANT_17_18 deposit expired during the month (closed months only). This column serves as the denominator (baseline) for calculating percentages related to user behavior at the end of their eligibility period (e.g., the proportion of users who diversified their bookings).
{% enddocs %}

{% docs column__metrics__total_deposit_amount_at_expiration %}
The total deposit amount allocated to beneficiaries that reached its expiration date during the given month.
{% enddocs %}

{% docs column__metrics__total_actual_amount_spent_at_expiration %}
The total deposit amount effectively spent by beneficiaries before it expired.
{% enddocs %}

{% docs column__metrics__average_deposit_amount_at_expiration %}
The average deposit amount allocated per beneficiary at the time of expiration. Do not average it across rows: recompute it from the totals.
{% enddocs %}

{% docs column__metrics__average_actual_amount_spent_at_expiration %}
The average deposit amount effectively spent per beneficiary at the time of expiration. Do not average it across rows: recompute it from the totals.
{% enddocs %}

{% docs column__metrics__total_active_partners_individual %}
Number of distinct partners (venues) with at least one bookable individual offer during the 365 days up to the end of the month (today for the current month).
{% enddocs %}

{% docs column__metrics__total_active_partners_collective %}
Number of distinct partners (venues) with at least one bookable collective offer during the 365 days up to the end of the month (today for the current month).
{% enddocs %}

{% docs column__metrics__total_active_partners_global %}
Number of distinct partners (venues) with at least one bookable offer, individual or collective, during the 365 days up to the end of the month (today for the current month).
{% enddocs %}

{% docs column__metrics__total_active_partners_dual_part %}
The total number of unique partners who had at least one active offer within both the individual part and the collective part simultaneously during the 365 days prior to the calculation date.
{% enddocs %}

{% docs column__metrics__total_cumulative_activated_partners_individual %}
The total cumulative number of unique partners who have had at least one active offer within the individual part for at least 1 day since inception up to the given month.
{% enddocs %}

{% docs column__metrics__total_cumulative_activated_partners_collective %}
The total cumulative number of unique partners who have had at least one active offer within the collective part for at least 1 day since inception up to the given month.
{% enddocs %}

{% docs column__metrics__total_cumulative_activated_partners_global %}
The total cumulative number of unique partners who have had at least one active offer within either the individual part, the collective part, or both, for at least 1 day since inception up to the given month.
{% enddocs %}

{% docs column__metrics__total_cumulative_activated_partners_individual_only %}
The total cumulative number of unique partners who have had at least one active offer exclusively within the individual part for at least 1 day, and have never had an active offer within the collective part since inception up to the given month.
{% enddocs %}

{% docs column__metrics__total_cumulative_activated_partners_collective_only %}
The total cumulative number of unique partners who have had at least one active offer exclusively within the collective part for at least 1 day, and have never had an active offer within the individual part since inception up to the given month.
{% enddocs %}

{% docs column__metrics__total_cumulative_activated_partners_dual_part %}
The total cumulative number of unique partners who have had at least one active offer within both the individual part and the collective part (at any point, concurrently or sequentially) for at least 1 day since inception up to the given month.
{% enddocs %}
{% docs column__total_institutions %} Total number of distinct educational institutions that made at least one collective booking. {% enddocs %}
{% docs column__total_collective_bookings_by_domain %} Total number of EAC collective bookings for the given institution geography and educational domain in the specified month. {% enddocs %}
{% docs column__total_collective_booking_amount_by_domain %} Total booking amount for EAC collective bookings for the given institution geography and educational domain in the specified month. {% enddocs %}
{% docs column__total_collective_tickets_by_domain %} Total number of tickets generated for EAC collective bookings for the given institution geography and educational domain in the specified month. {% enddocs %}
{% docs column__total_collective_institutions_by_domain %} Total number of distinct educational institutions with at least one EAC collective booking for the given geography and educational domain in the specified month. {% enddocs %}

{% docs column__cumulative_total_collective_bookings_by_domain %} Cumulative total number of EAC collective bookings for the same institution geography and domain from the start of the scholar year up to and including the specified month. {% enddocs %}
{% docs column__cumulative_total_collective_booking_amount_by_domain %} Cumulative total booking amount for EAC collective bookings for the same institution geography and domain from the start of the scholar year up to and including the specified month. {% enddocs %}
{% docs column__cumulative_total_collective_tickets_by_domain %} Cumulative total number of tickets generated for EAC collective bookings for the same institution geography and domain from the start of the scholar year up to and including the specified month. {% enddocs %}
{% docs column__cumulative_total_collective_bookings_by_format %} Cumulative total number of EAC collective bookings for the same institution geography and offer format from the start of the scholar year up to and including the specified month. {% enddocs %}
{% docs column__cumulative_total_collective_booking_amount_by_format %} Cumulative total booking amount for EAC collective bookings for the same institution geography and offer format from the start of the scholar year up to and including the specified month. {% enddocs %}
{% docs column__cumulative_total_collective_tickets_by_format %} Cumulative total number of tickets generated for EAC collective bookings for the same institution geography and offer format from the start of the scholar year up to and including the specified month. {% enddocs %}
{% docs column__cumulative_total_collective_institutions_by_format %} Number of distinct educational institutions with at least one EAC collective booking in the offer format since the start of the scholar year, up to and including the specified month (each institution counted once). {% enddocs %}
{% docs column__cumulative_total_collective_institutions_by_domain %} Number of distinct educational institutions with at least one EAC collective booking in the domain since the start of the scholar year, up to and including the specified month (each institution counted once). {% enddocs %}

{% docs column__metrics__total_institutions %}
Total number of institutions (schools) registered or eligible within the territory (at the given geographical granularity), for the specified month and school year.
{% enddocs %}

{% docs column__metrics__total_engaged_institutions %}
Total cumulative number of "engaged" institutions (having made at least one booking) within the territory for the specified month and school year.
{% enddocs %}

{% docs column__metrics__total_deposit %}
Total credit amount allocated and made available to the institutions of the territory for the specified month and school year.
{% enddocs %}

{% docs column__metrics__total_amount_spent %}
Total amount spent (consumed) by the institutions of the territory during the specified month and school year.
{% enddocs %}

{% docs column__metrics__total_bookings %}
Total number of bookings made by the institutions or beneficiaries of the territory during the specified month and school year.
{% enddocs %}

{% docs column__metrics__cumulative_total_amount_spent %}
Cumulative total amount spent by the institutions of the territory from the start of the given school year up to and including the specified month.
{% enddocs %}

{% docs column__metrics__cumulative_total_bookings %}
Cumulative total number of bookings made within the territory from the start of the given school year up to and including the specified month.
{% enddocs %}

{% docs column__total_mau %}
Total Monthly Active Users (MAU): the count of unique beneficiaries who visited the platform during the month.
{% enddocs %}

{% docs column__avg_dau %}
Average Daily Active Users (DAU): the average number of unique active beneficiaries per day over the course of the month.
{% enddocs %}

{% docs column__pct_stickiness_dau_mau_ratio %}
User stickiness ratio calculated as `(avg_dau / total_mau)`, representing how frequently monthly active users return to the platform on a daily basis.
{% enddocs %}

{% docs column__total_eligible_users %}
Total number of beneficiaries eligible with at least 5 euros remaining credit to connect and use their pass Culture credit during the month.
{% enddocs %}

{% docs column__total_connected_users %}
Total number of eligible beneficiaries who successfully logged in or connected to the platform at least once during the month.
{% enddocs %}

{% docs column__pct_connected_users %}
Share of eligible beneficiaries who connected to the platform during the month (`total_connected_users / total_eligible_users`), as a ratio between 0 and 1 (not multiplied by 100).
{% enddocs %}

{% docs column__total_onboarding_users %}
Total number of users who initiated the onboarding process during the signup week.
{% enddocs %}

{% docs column__total_validated_7d_users %}
Total number of users who successfully completed and validated their identity/onboarding within 7 days of initiating it.
{% enddocs %}

{% docs column__total_registered_beneficiaries %}
Total number of fully registered beneficiaries whose accounts became active during the signup week.
{% enddocs %}

{% docs column__total_activated_7d_beneficiaries %}
Total number of registered beneficiaries who received their deposit within 7 days following their registration.
{% enddocs %}

{% docs column__pct_activated_7d_beneficiaries %}
Percentage of registered beneficiaries who were activated within 7 days (`total_activated_7d_beneficiaries / total_registered_beneficiaries`).
{% enddocs %}

{% docs column__pct_validated_7d_users %}
Percentage of onboarding users who validated their onboarding process within 7 days (`total_validated_7d_users / total_onboarding_users`).
{% enddocs %}

{% docs column__discoverability_month %} First day of the calendar month the row aggregates (bookability and consultation counts are computed per month). {% enddocs %}

{% docs column__nb_bookable_offers %} Number of distinct offers of this item that were bookable at least once during the month. {% enddocs %}
{% docs column__nb_bookable_days %} Number of distinct days during the month on which at least one offer of this item was bookable. {% enddocs %}

{% docs column__nb_monthly_consult %} Total number of consultations of this item recorded during the month, all origins combined (Firebase offer-consultation events). {% enddocs %}
{% docs column__nb_monthly_search_consult %} Number of the item's monthly consultations that originated from search. {% enddocs %}
{% docs column__nb_monthly_home_consult %} Number of the item's monthly consultations that originated from the home page (`home`, `video`, `videoModal`, `highlightOffer`, `thematicHighlight` or `exclusivity` origins). {% enddocs %}
{% docs column__nb_monthly_venue_consult %} Number of the item's monthly consultations that originated from a venue page. {% enddocs %}
{% docs column__nb_monthly_favorites_consult %} Number of the item's monthly consultations that originated from the user's favorites. {% enddocs %}
{% docs column__nb_monthly_similar_offer_consult %} Number of the item's monthly consultations that originated from a similar-offer or same-artist-playlist recommendation. {% enddocs %}
{% docs column__nb_monthly_other_channel_offer_consult %} Number of the item's monthly consultations that originated from any channel other than search, home, venue, favorites or similar-offer recommendations. {% enddocs %}

{% docs column__cnt_events %} Number of consultation events matching this row's dimensions (event date, offer, origin, traffic attribution, user role/age). {% enddocs %}

{% docs column__metrics__total_offerers_created_by_cohort %}
Total number of offerers registered during the cohort month, anchored to the geography of their first venue. Cohorts whose 30-day activation window has not yet closed are excluded.
{% enddocs %}

{% docs column__metrics__total_activated_offerer_consultation_or_adage_30d_by_cohort %}
Number of offerers from the cohort who, within 30 days of registration, either had their first offer consulted by a beneficiary OR submitted a first Adage DMS application. Offerers already referenced in Adage without a DMS application count as activated at day 0.
{% enddocs %}

{% docs column__metrics__total_activated_offerer_offer_or_adage_30d_by_cohort %}
Number of offerers from the cohort who, within 30 days of registration, either created their first individual offer OR submitted a first Adage DMS application. Offerers already referenced in Adage without a DMS application count as activated at day 0.
{% enddocs %}

{% docs column__metrics__total_activated_offerer_global_30d_by_cohort %}
Number of offerers from the cohort who created at least one individual or collective offer within 30 days of registration.
{% enddocs %}

{% docs column__metrics__total_activated_offerer_individual_30d_by_cohort %}
Number of offerers from the cohort who created at least one individual offer within 30 days of registration.
{% enddocs %}

{% docs column__metrics__total_activated_offerer_collective_30d_by_cohort %}
Number of offerers from the cohort who created at least one collective offer within 30 days of registration.
{% enddocs %}

{% docs column__total_active_partners_count_department %}
Number of distinct cultural partners with at least one used booking in the department over the rolling window.
{% enddocs %}

{% docs column__max_partner_revenue_share_department %}
Share of total department revenue held by the top cultural partner over the rolling window (value between 0 and 1).
{% enddocs %}

{% docs column__total_revenue_amount_department %}
Total revenue generated by used bookings in the department over the rolling window.
{% enddocs %}

{% docs column__total_active_partners_count_epci %}
Number of distinct cultural partners with at least one used booking in the EPCI over the rolling window.
{% enddocs %}

{% docs column__max_partner_revenue_share_epci %}
Share of total EPCI revenue held by the top cultural partner over the rolling window (value between 0 and 1).
{% enddocs %}

{% docs column__total_revenue_amount_epci %}
Total revenue generated by used bookings in the EPCI over the rolling window.
{% enddocs %}

{% docs column__total_active_partners_count_municipality %}
Number of distinct cultural partners with at least one used booking in the municipality over the rolling window.
{% enddocs %}

{% docs column__max_partner_revenue_share_municipality %}
Share of total municipality revenue held by the top cultural partner over the rolling window (value between 0 and 1).
{% enddocs %}

{% docs column__perturbation_table__count_min %}
Lower bound (inclusive) of the range of exact counts this perturbation row applies to.
{% enddocs %}

{% docs column__perturbation_table__count_max %}
Upper bound (inclusive) of the range of exact counts this perturbation row applies to.
{% enddocs %}

{% docs column__perturbation_table__cell_key_min %}
Lower bound (inclusive) of the range of cell keys (0 to 255) this perturbation row applies to.
{% enddocs %}

{% docs column__perturbation_table__cell_key_max %}
Upper bound (inclusive) of the range of cell keys (0 to 255) this perturbation row applies to.
{% enddocs %}

{% docs column__perturbation_table__perturbation %}
Noise added to the exact count (cell key method, max noise 5, no published count between 1 and 4). The perturbed count is greatest(count + perturbation, 0), for the row where count is between count_min and count_max and the cell key between cell_key_min and cell_key_max.
{% enddocs %}

{% docs column__cell_key %}
Cell key (0-255) of the matching count, used to perturb that count on publication (statistical disclosure control, cell key method): the salted record keys of the counted users summed modulo 256. Neither a measure nor a dimension: do not display it. When rows are aggregated, the key of the aggregate is mod(abs(sum(cell_key)), 256), not the plain sum.
{% enddocs %}
