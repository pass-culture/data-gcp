## Introduction

The glossary provides a comprehensive list of terms and definitions used in the data model.
The glossary is organized by concept and includes terms from all data models.

A column in a model is usually start by the concept it refers.
For example in case of `offer_id`, `booking_is_used`, `booking_cancellation_date` the associated concept will be `offer`.

## Applicative

As for the models, `applicative` means that the term or concept is associated with data that is handle by internal systems that handle the business logic of the application.
For example, `booking`, `user`, `deposit`, `stock`.

## Event

We also track events within the distinct applications (native, pro, adage) on distinct platform (web, mobile).
The event concept usually refers to data that comes from our tracking systems.
It is used to track user interactions within the application.
It can be directly on the app (via firebase) our through backend logs.

Columns or table like `event_name`, `event_date`, `event_type`, `event_value` are associated with this event concept.
Columns or tables like `log` are usually associated with logs from backend systems.

## Other

We also ingest data from other external sources which can be described in distinct folder per origin source.
