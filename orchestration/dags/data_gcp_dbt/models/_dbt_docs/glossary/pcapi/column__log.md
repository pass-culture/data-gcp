!!! note
    This file is auto-generated

    :construction_worker_tone1: Work in progress :construction_worker_tone1:


*Base columns for log entries coming from pcapi backend logging.*

{% docs column__log_timestamp %}The timestamp when the log entry was recorded.{% enddocs %}
{% docs column__partition_date %}The date used for partitioning the log data.{% enddocs %}
{% docs column__environement %}The environment in which the log entry was recorded, such as production or staging.{% enddocs %}
{% docs column__message %}The message content of the log entry, describing the event or action.{% enddocs %}
{% docs column__technical_message_id %}The technical identifier for the message associated with the log entry.{% enddocs %}
{% docs column__device_id %}The identifier for the device used during the log entry event.{% enddocs %}
{% docs column__analytics_source %}The source of analytics data, such as "adage" "backoffice", "app-pro", "native" associated with the log entry.{% enddocs %}
{% docs column__choice_datetime %}The timestamp when the cookie conset was recorded.{% enddocs %}

{% hide columns %}

**Additional specific columns**

*Theses columns are associated with logs for specific events or actions in order to assist fraud detection, tracking, and analysis.*

{% docs column__stock_old_quantity %}The previous quantity of the stock before the log entry event.{% enddocs %}
{% docs column__stock_new_quantity %}The new quantity of the stock after the log entry event.{% enddocs %}
{% docs column__stock_old_price %}The previous price of the stock before the log entry event.{% enddocs %}
{% docs column__stock_new_price %}The new price of the stock after the log entry event.{% enddocs %}
{% docs column__stock_booking_quantity %}The quantity of stock booked during the log entry event.{% enddocs %}
{% docs column__list_of_eans_not_found %}A list of EANs (European Article Numbers) that were not found during the log entry event, offer creation or synchronisation.{% enddocs %}
{% docs column__beta_test_new_nav_is_convenient %}Feedback on whether navigation on the new pro website is convenient, collected during beta testing (04/2024-11/2024).{% enddocs %}
{% docs column__beta_test_new_nav_is_pleasant %}Feedback on whether navigation on the new pro website is pleasant, collected during beta testing (04/2024-11/2024).{% enddocs %}
{% docs column__beta_test_new_nav_comment %}Textual reviews on the new pro website interface, collected from users during beta testing (04/2024-11/2024).{% enddocs %}
{% docs column__cookies_consent_mandatory %}Indicates whether cookies consent is mandatory for the user.{% enddocs %}
{% docs column__cookies_consent_accepted %}Indicates whether the user accepted cookies consent.{% enddocs %}
{% docs column__cookies_consent_refused %}Indicates whether the user refused cookies consent.{% enddocs %}
{% docs column__user_satisfaction %}Textual reviews on the pro website interface and navigation, collected from November 2024.{% enddocs %}
{% docs column__user_comment %}Multi-choice feedback on pro website from very bad to excellent, collected from November 2024.{% enddocs %}
{% docs column__suggested_offer_api_id %}API call ID as part of individual offer creation predictiv model of suggested subcategories.{% enddocs %}
{% docs column__suggested_offer_api_subcategory %}Subcategory chosen by the user as part of individual offer creation(predictiv model).{% enddocs %}
{% docs column__suggested_offer_api_subcategories %}Subcategories suggested to the user as part of individual offer creation (predictiv model).{% enddocs %}
{% docs column__author_id %}The identifier of the author responsible for the action (could be a user, admin, or automated process).{% enddocs %}
{% docs column__is_user_connect_as %}Indicates whether the user is connected on behalf of another account (impersonation).{% enddocs %}
{% docs column__stock_old_price_category %}The previous price category before the log entry event.{% enddocs %}
{% docs column__stock_new_price_category %}The new price category after the log entry event.{% enddocs %}
{% docs column__publication_date_old_value %}The previous publication date of the offer before modification.{% enddocs %}
{% docs column__publication_date_new_value %}The new publication date of the offer after modification.{% enddocs %}
{% docs column__booking_limit_date_old_value %}The previous booking limit date of the offer before modification.{% enddocs %}
{% docs column__booking_limit_date_new_value %}The new booking limit date of the offer after modification.{% enddocs %}
{% docs column__stock_beginning_date_old_value %}The previous beginning date of the stock before modification.{% enddocs %}
{% docs column__stock_beginning_date_new_value %}The new beginning date of the stock after modification.{% enddocs %}
{% docs column__offer_withdrawal_details_old_value %}The previous withdrawal details of the offer before modification.{% enddocs %}
{% docs column__offer_withdrawal_details_new_value %}The new withdrawal details of the offer after modification.{% enddocs %}
{% docs column__offerer_address_old_value %}The previous address (performance venue) of the offer/stock before modification, as recorded in the system.{% enddocs %}
{% docs column__offerer_address_new_value %}The new address (performance venue) of the offer/stock after modification, as updated in the system.{% enddocs %}


{% endhide %}
