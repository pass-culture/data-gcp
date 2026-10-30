The `int_brevo__transactional_pro` table holds event-level data from Brevo for transactional emails sent to cultural partners. Each row represents one recipient (`email_id`, a hash of the recipient address) of a template and tag, for one offerer and one event day. Counts of delivered, opened and unsubscribed events are summed over every import covering that day. A recipient address linked to several offerers appears once per offerer.

This dataset can be used to monitor the effectiveness of email campaigns, evaluate offerer behavior, and identify trends or opportunities to enhance engagement strategies.

## Table description

| name               | data_type | description                                                                                       |
| ------------------ | --------- | ------------------------------------------------------------------------------------------------- |
| brevo_tag          |           | Tag of the brevo transactional email. It allows to identify                                       |
| brevo_template_id  |           | Brevo template id                                                                                 |
| target             |           | Audience of the Brevo transactional email: 'native' (beneficiaries) or 'pro' (cultural partners). |
| email_id           |           | Recipient identifier: SHA-256 hash (hex) of the recipient email address.                          |
| offerer_id         |           | Unique identifier of the offerer.                                                                 |
| user_id            |           | Unique identifier for a user.                                                                     |
| event_date         |           | The date when the event occurred, used for partitioning the data.                                 |
| total_delivered    |           | Count of emails successfully delivered to the recipient.                                          |
| total_opened       |           | Count of emails opened by the recipient.                                                          |
| total_unsubscribed |           | Count of recipients who unsubscribed after receiving the email.                                   |
