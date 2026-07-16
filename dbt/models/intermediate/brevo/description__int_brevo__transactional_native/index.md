The `int_brevo__transactional_native` table event-level data from Brevo, providing insights into user interactions and email campaign performance. Each row represents a specific event associated with a user, linked to a particular email template and categorized with tags. The table contains metrics such as the number of emails delivered, opened, and unsubscribed, enabling a detailed analysis of email engagement.

In addition to email-related metrics, the table provides user-specific engagement data, such as the number of sessions, consultations offered, bookings made, and items marked as favorites in the app. The inclusion of the user_current_deposit_type column allows for segmentation and analysis based on user-specific attributes at the time of the event.

This dataset can be used to monitor the effectiveness of email campaigns, evaluate user behavior, and identify trends or opportunities to enhance engagement strategies.

## Table description

| name                      | data_type | description                                                                                          |
| ------------------------- | --------- | ---------------------------------------------------------------------------------------------------- |
| user_id                   |           | Unique identifier for a user.                                                                        |
| template_id               |           | Brevo template id                                                                                    |
| tag                       |           | Tag of the brevo transactional email. It allows to identify                                          |
| event_date                |           | The date when the event occurred, used for partitioning the data.                                    |
| total_delivered           |           | Count of emails successfully delivered to the recipient.                                             |
| total_opened              |           | Count of emails opened by the recipient.                                                             |
| total_unsubscribed        |           | Count of recipients who unsubscribed after receiving the email.                                      |
| user_current_deposit_type |           | Type of the user's current deposit.                                                                  |
| session_number            |           | The number of the session during which the visit occurred, indicating the sequence of user sessions. |
| offer_consultation_number |           | The number of distinct consultation occurences of an offer from home playlists on a given date.      |
| booking_number            |           | Total number of bookings, both individual and collective.                                            |
| favorites_number          |           | Total number of times this offer was favorited.                                                      |
