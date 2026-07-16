# Table: Favorite

This model exports favorite data for ministry use. It contains information about user favorites. Currently, only offers can be added to favorites. All user types (whether they have received a grant or not) can add offer to favorites. The model includes :

- Favorite identifier
- Creation date
- Associated user and offer

## Table description

| name                | data_type | description                                                                                     |
| ------------------- | --------- | ----------------------------------------------------------------------------------------------- |
| favorite_id         |           | The unique identifier for the favorite. You can save multiple times a single offer as favorite. |
| favorite_created_at |           | Timestamp when the favorite was created.                                                        |
| user_id             |           | Unique identifier for a user.                                                                   |
| offer_id            |           | Unique identifier for the offer.                                                                |
