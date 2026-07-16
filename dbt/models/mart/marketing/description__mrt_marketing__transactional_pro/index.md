The `mrt_marketing__transactional_pro` table event-level data from Brevo, providing insights into offerer interactions and email campaign performance. Each row represents a specific event associated with a offerer, linked to a particular email template and categorized with tags. The table contains metrics such as the delivering, opening, and unsubscribtions, enabling a detailed analysis of email engagement.

This dataset can be used to monitor the effectiveness of email campaigns, evaluate offerer behavior, and identify trends or opportunities to enhance engagement strategies.

## Table description

| name                  | data_type | description                                                                |
| --------------------- | --------- | -------------------------------------------------------------------------- |
| brevo_tag             |           | Tag of the brevo transactional email. It allows to identify                |
| brevo_template_id     |           | Brevo template id                                                          |
| offerer_id            |           | Unique identifier of the offerer.                                          |
| event_date            |           | The date when the event occurred, used for partitioning the data.          |
| email_is_delivered    |           | Boolean. True if the email has been delivered, else False.                 |
| email_is_opened       |           | Boolean. True if the email has been opened, else False.                    |
| user_has_unsubscribed |           | Boolean. True if the email encouraged the user to unsubscribe, else False. |
