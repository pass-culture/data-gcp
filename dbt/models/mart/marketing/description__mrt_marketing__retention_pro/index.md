The `mrt_marketing__retention_pro` is an offerer-level table which aims at analyzing the impact of transactional campaigns (+ newsletters soon) from Brevo on offerer retention (bookable offers and bookings).

## Table description

Each line is retention datas of a single offerer, which received and openened at least one retention email.

Why studying retention of offerers while we mainly send campaigns to venues ? Unfortunately, the only data provided by Brevo for a mail sent is the email address, which can either be a user email, a venue booking email, a venue collective booking email, a venue contact email... However, one email can be linked to several venues of a single offerer : that's why we cannot clearly specify which venue the mail targets. The offerer is the only granularity we can rely on.

| name                                      | data_type | description                                                                                                          |
| ----------------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------- |
| offerer_id                                |           | Unique identifier of the offerer.                                                                                    |
| brevo_tag                                 |           | Tag of the brevo transactional email. It allows to identify                                                          |
| offerer_name                              |           | Name of the offerer.                                                                                                 |
| offerer_creation_date                     |           | Date when the offerer was created. Equals to the registration date of the offerer on the professional portal.        |
| first_open_date                           |           | Date on which a user of this offerer opened a mail from the campaign (brevo tag) for the first time.                 |
| last_open_date                            |           | Date on which a user of this offerer opened a mail from the campaign (brevo tag) for the last time.                  |
| nb_open_days                              |           | Number distinct days on which a user of this offerer opened a mail from the campaign (brevo tag).                    |
| last_individual_offer_creation_date       |           | Date of the last individual offer creation.                                                                          |
| last_collective_offer_creation_date       |           | Date of the last collective offer creation.                                                                          |
| last_individual_bookable_offer_date       |           | Date of the last individual bookable offer.                                                                          |
| last_collective_bookable_offer_date       |           | Date of the last collective bookable offer.                                                                          |
| first_individual_bookable_date_after_mail |           | First day on which the offerer has a bookable individual offer after he opened a mail from the campaign (brevo tag). |
| first_collective_bookable_date_after_mail |           | First day on which the offerer has a bookable collective offer after he opened a mail from the campaign (brevo tag). |
| last_individual_booking_date              |           | Date of the last individual booking.                                                                                 |
| last_collective_booking_date              |           | Date of the last collective booking.                                                                                 |
