The `int_brevo__transactional_pro` table event-level data from Brevo, providing insights into offerer interactions and email campaign performance. Each row represents a specific event associated with a offerer, linked to a particular email template and categorized with tags. The table contains metrics such as the number of emails delivered, opened, and unsubscribed, enabling a detailed analysis of email engagement.

This dataset can be used to monitor the effectiveness of email campaigns, evaluate offerer behavior, and identify trends or opportunities to enhance engagement strategies.

## Table description

| name               | data_type | description                                                       |
| ------------------ | --------- | ----------------------------------------------------------------- |
| tag                |           | Tag of the brevo transactional email. It allows to identify       |
| template           |           | Brevo template id                                                 |
| email_id           |           | Brevo email sent id                                               |
| venue_id           |           | Unique identifier for the venue.                                  |
| event_date         |           | The date when the event occurred, used for partitioning the data. |
| total_delivered    |           | Count of emails successfully delivered to the recipient.          |
| total_opened       |           | Count of emails opened by the recipient.                          |
| total_unsubscribed |           | Count of recipients who unsubscribed after receiving the email.   |
