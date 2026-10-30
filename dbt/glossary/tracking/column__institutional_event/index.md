**institutional_event_user_first_touch_timestamp**: Timestamp of the first visit of the browser (user_pseudo_id) on the institutional website.

**institutional_event_device_hostname**: Hostname of the institutional website on which the event was recorded (pass.culture.fr, or the testing website).

**institutional_event_geo_region**: Region of the user, as geolocated by Google Analytics from the IP address.

**institutional_event_geo_city**: City of the user, as geolocated by Google Analytics from the IP address.

**institutional_event_user_traffic_campaign**: Name of the marketing campaign that first acquired the user (first-touch attribution).

**institutional_event_user_traffic_medium**: Medium of the marketing campaign that first acquired the user (first-touch attribution).

**institutional_event_user_traffic_source**: Source of the marketing campaign that first acquired the user (first-touch attribution).

**institutional_event_ga_session_id**: Google Analytics session identifier (session start unix timestamp). Unique only when combined with user_pseudo_id.

**institutional_event_engagement_time_msec**: Time in milliseconds the page was in the foreground since the previous engagement event.

**institutional_event_entrances**: Equals 1 when the page view is the first page of the session (landing page).

**institutional_event_percent_scrolled**: Percentage of the page scrolled, set on scroll events. Always 90: GA4 sends the scroll event once the user reaches 90% of the page.

**institutional_event_engaged_session_event**: Equals 1 when the event belongs to an engaged session.

**institutional_event_session_engaged**: '1' when the session is engaged (lasted more than 10 seconds, had 2+ page views or a key event), '0' otherwise.

**institutional_event_page_title**: Title of the page on which the event occurred.

**institutional_event_origin**: Path of the page on which the event occurred, sent by the institutional website tracking plan.

**institutional_event_source**: Source of the marketing campaign that generated the session (utm_source).

**institutional_event_medium**: Medium of the marketing campaign that generated the session (utm_medium).

**institutional_event_campaign**: Name of the marketing campaign that generated the session (utm_campaign).

**institutional_event_term**: Paid search keyword of the marketing campaign that generated the session (utm_term).

**institutional_event_ignore_referrer**: Equals 'true' when the referrer is ignored for attribution (internal or excluded referral).

**institutional_event_link_url**: URL of the clicked link, set on outbound click and file_download events.

**institutional_event_link_domain**: Domain of the clicked outbound link.

**institutional_event_link_text**: Text of the clicked link, set on file_download events.

**institutional_event_link_classes**: CSS classes of the clicked link element.

**institutional_event_outbound**: Equals 'true' when the clicked link leads to another domain.

**institutional_event_file_name**: Path of the downloaded file, set on file_download events.

**institutional_event_file_extension**: Extension of the downloaded file, set on file_download events.

**institutional_event_privacy_consent_type**: Consent flag sent with the cookie banner answer ('true' or 'false'), set on consent.answer events.

**institutional_event_privacy_consent_value**: Answer given by the user to the cookie banner ('full' or 'refusal'), set on consent.answer events.
