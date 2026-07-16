# Overview

This table contains all survey responses from Zendesk, a customer service software platform that we use to manage support tickets and customer interactions.

## Key Concepts

### Survey Responses

A survey response represents feedback provided by a customer after a support interaction. Each response is linked to a specific ticket and contains:

- A unique survey response ID
- The associated ticket ID
- The customer (requester) and support agent (assignee) IDs
- A satisfaction score
- Timestamps for creation and updates

## Table description

| name                         | data_type | description                                                      |
| ---------------------------- | --------- | ---------------------------------------------------------------- |
| survey_response_id           |           | The unique identifier for each Zendesk survey response.          |
| survey_response_created_at   |           | The timestamp when the Zendesk survey response was created.      |
| ticket_id                    |           | The unique identifier for each Zendesk ticket.                   |
| ticket_requester_id          |           | The unique identifier for the requester of the Zendesk ticket.   |
| ticket_assignee_id           |           | The unique identifier for the assignee of the Zendesk ticket.    |
| survey_response_score        |           | The score of the Zendesk survey response.                        |
| survey_response_updated_at   |           | The timestamp when the Zendesk survey response was last updated. |
| survey_response_created_date |           | The date when the Zendesk survey response was created.           |
