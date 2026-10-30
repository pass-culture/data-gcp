The `mrt_marketing__transactional_pro` table aggregates Brevo transactional email events sent to cultural partners. Each row represents one tag, template, offerer and event day. The boolean columns are true when at least one recipient of the offerer was delivered, opened or unsubscribed; the `total_recipients_*` columns count the distinct recipients who did.

This dataset can be used to monitor the effectiveness of email campaigns, evaluate offerer behavior, and identify trends or opportunities to enhance engagement strategies.

## Table description

| name                          | data_type | description                                                                |
| ----------------------------- | --------- | -------------------------------------------------------------------------- |
| brevo_tag                     |           | Tag of the brevo transactional email. It allows to identify                |
| brevo_template_id             |           | Brevo template id                                                          |
| offerer_id                    |           | Unique identifier of the offerer.                                          |
| event_date                    |           | The date when the event occurred, used for partitioning the data.          |
| email_is_delivered            |           | Boolean. True if the email has been delivered, else False.                 |
| email_is_opened               |           | Boolean. True if the email has been opened, else False.                    |
| user_has_unsubscribed         |           | Boolean. True if the email encouraged the user to unsubscribe, else False. |
| total_recipients_delivered    |           | Number of distinct recipients to whom at least one email was delivered.    |
| total_recipients_opened       |           | Number of distinct recipients who opened at least one email.               |
| total_recipients_unsubscribed |           | Number of distinct recipients who unsubscribed after receiving the email.  |
