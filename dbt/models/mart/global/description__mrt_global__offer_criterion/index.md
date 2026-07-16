The `mrt_global__offer_criterion` table captures data related to criterion (tags) and their related offers.

## Table description

Maps the offers and their associated tags. There are different types of tags used for different purposes by different teams: offers listings for editorial playlists, for example. One offer can have several tags.

| name                     | data_type | description                                                                                                                    |
| ------------------------ | --------- | ------------------------------------------------------------------------------------------------------------------------------ |
| criterion_id             |           | The unique identifier of the criterion.                                                                                        |
| tag_name                 |           | The name of the criterion.                                                                                                     |
| description              |           | The description of the criterion.                                                                                              |
| criterion_category_label |           | The category label of the offer criterion: specifies the objective of the criterion : internal process, editorialization, etc. |
| criterion_beginning_date |           | The beginning date of the criterion's validity.                                                                                |
| criterion_ending_date    |           | The ending date of the criterion's validity.                                                                                   |
| offer_id                 |           | Unique identifier for the offer.                                                                                               |
| offer_name               |           | Name of the offer as it appears in the application.                                                                            |
| highlight_id             |           | Unique identifier of the highlight itself ("temps de valorisation thématique").                                                |
