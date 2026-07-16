# Table: Global Artist

The `mrt_global__artist` table is designed to store comprehensive information about artist representation on the application.

The Product team and Data Science team have worked to relate offers to artists in order to link the offers related to the same artist and provide more relevant suggestions to users.

The artist_id is a home-made id that caracterizes artists with a particular artist_type and wikidatas related to these artists. Some artist_id can be related to different artist_types.

## Table description

This model agregates the use of the application by artist_id, in order to be able to analyse and rank them. It shows metrics related to consultation, booking and offers created by artist for example.

| name                                                 | data_type | description                                                                                                  |
| ---------------------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------ |
| artist_id                                            | STRING    | Unique identifier of the artist.                                                                             |
| artist_name                                          | STRING    | Name of the artist.                                                                                          |
| artist_description                                   | STRING    | Short description of the artist.                                                                             |
| wikidata_image_file_url                              | STRING    | URL of the image file from Wikidata.                                                                         |
| wikidata_image_license                               | STRING    | License of the image from Wikidata.                                                                          |
| wikidata_image_license_url                           | STRING    | URL of the image license from Wikidata.                                                                      |
| wikidata_image_author                                | STRING    | Author of the image from Wikidata.                                                                           |
| artist_creation_date                                 | DATETIME  | Creation date of the artist.                                                                                 |
| artist_modification_date                             | DATETIME  | Date of the last modification of the artist.                                                                 |
| artist_total_consultations                           |           | Total consultations of offers related to the artist, after January 2025.                                     |
| artist_total_consultations_from_search               |           | Total consultations of offers related to the artist that came from search, after January 2025.               |
| artist_total_consultations_from_offer                |           | Total consultations of offers related to the artist that came from offer page, after January 2025.           |
| artist_total_consultations_from_venue                |           | Total consultations of offers related to the artist that came from venue page, after January 2025.           |
| artist_total_consultations_from_search_auto_complete |           | Total consultations of offers related to the artist that came from search auto complete, after January 2025. |
| artist_total_consulted_users                         |           | Total users that consulted offers related to the artist, after January 2025.                                 |
| artist_total_products                                |           | Total products created related to the artist.                                                                |
| artist_total_offers                                  |           | Total offers created related to the artist.                                                                  |
| artist_total_bookable_offers                         |           | Total bookable offers created related to the artist.                                                         |
| artist_total_offer_categories                        |           | Total distinct categories of offers created related to the artist.                                           |
| artist_total_venues                                  |           | Total venues that created offers related to the artist.                                                      |
| artist_total_artist_types                            |           | Total distinct type (author/performer) related to the artist in the base.                                    |
| artist_total_bookings                                |           | Total bookings on offers related to the artist.                                                              |
