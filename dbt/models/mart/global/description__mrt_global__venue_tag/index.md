The `mrt_global__venue_tag` table gathers all venue tags manually applied by our teams, in order to count and track the activity of cultural partners on a more granular level : by Ministry of Culture's labels, type of activity, playlist displayed on the Native App.

Our teams developed theses tags because :

- product data and NAF code do not include these informations or
- product data do share this information, but the declarative information is often erroneous

There are three types of venue tag (“venue_tag_category_label” fields) :

- "Comptage partenaire label et appellation du MC" : Ministry of Culture's labels, such as "Orchestre national", "Théâtre Lyrique", "Fonds régional d'art contemporain..'
- "Comptage partenaire sectoriel" : additional data on activity type of venues (CSTI, Education Populaire, MJC, Tiers-Lieu) which are not Ministry of Culture's labels
- "Playlist lieux et offres" : playlists which display the venue (/ offers of the venue) on the Native App. It enables editorial teams to measure the impact of display on venue activity (consultations, bookings..)

## Table description

| name                     | data_type | description                                                                                                                                                                              |
| ------------------------ | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| venue_id                 |           | Unique identifier for the venue.                                                                                                                                                         |
| offerer_id               |           | Unique identifier of the offerer.                                                                                                                                                        |
| partner_id               |           | Unique identifier of the partner.                                                                                                                                                        |
| offerer_rank_desc        |           | Offerer rank in descending order.                                                                                                                                                        |
| venue_tag_id             |           | The unique identifier for the venue tag.                                                                                                                                                 |
| venue_tag_category_id    |           | The unique identifier for the venue tag category.                                                                                                                                        |
| venue_tag_category_label |           | The label of the venue tag category : Comptage partenaire label et appellation du MC, Comptage partenaire sectoriel, or Playlist lieux et offres (more details in the table description) |
| venue_tag_name           |           | The name of the venue tag.                                                                                                                                                               |
