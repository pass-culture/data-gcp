---
title: Zendesk Ticket
description: Description of the `int_zendesk__ticket` table.
---

{% docs description__int_zendesk__ticket %}

# Overview

This table contains all tickets from Zendesk, a customer service software platform that we use to manage support tickets and customer interactions.

For more information about Zendesk, see the [Internal Job Zendesk API documentation](https://www.notion.so/passcultureapp/Zendesk-Ticket-1a4ad4e0ff988112aad5ecc4d1d1968f).

## Key Concepts

### Tickets
A ticket represents a conversation between a customer and the support team and contains metadata about the ticket and the customer.


### Tags and Categorization
Tickets are organized using:
- Support typology (native and professional)
- Technical categories
- Fraud detection flags
- Priority levels

### User_id

- We use the email address of the customer to link it to the user_id if any.

### Technical Partner

- The technical partner is a tag that is aposed by the support team to flag B2B interractions with API.

### Typology

- The typology is a tag that is aposed by the support team to flag the type of support requested by the customer.
- Each ticket can have multiple typologies. We export only native and pro typologies.


{% enddocs %}

## Table description

{% docs table__int_zendesk__ticket %}{% enddocs %}
