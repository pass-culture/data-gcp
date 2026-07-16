# Special event Model

The `mrt_global__special_event` table aims to register and analyse users' application to special events. Special events are unique experiences developed for pass Culture beneficiaries in partnership with cultural actors, for which a lottery or a selection process based on motivation and objective criteria may be conducted.

\
**Business Rules**

- user_id can be empty field if the declarative email does not match any registered user.

## Table description

| name                                  | data_type | description                                                                                         |
| ------------------------------------- | --------- | --------------------------------------------------------------------------------------------------- |
| special_event_id                      |           | Unique identifier of a special event .                                                              |
| special_event_title                   |           | Title of the special event .                                                                        |
| offerer_id                            |           | Unique identifier of the offerer.                                                                   |
| venue_id                              |           | Unique identifier for the venue.                                                                    |
| typeform_id                           |           | Unique typeform_id related to a unique special event id. This id is used to link contentful datas . |
| special_event_creation_date           |           | Date when the special event was created .                                                           |
| special_event_date                    |           | Date when the special event will occur .                                                            |
| special_event_response_id             |           | Unique identifier of a response of a user to a special event .                                      |
| user_id                               |           | Unique identifier for a user.                                                                       |
| special_event_response_status         |           | Status of the response to the user's candidacy to a special event .                                 |
| special_event_response_submitted_date |           | Date when the response was given to the user .                                                      |
| special_event_end_date                |           | Date when the special event will end.                                                               |
