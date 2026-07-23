# Table: Future Artist

The `ml_linkage__future_artist` table contains a preview of the artists once ingestion of the delta artist is completed.

This table is used to validate that the future state of the artists is correct before synchronizing with the backend application by running dbt tests on it.

## Table description

| name                       | data_type | description                                                                                  |
| -------------------------- | --------- | -------------------------------------------------------------------------------------------- |
| artist_id                  |           | Unique identifier of the artist.                                                             |
| artist_name                |           | Name of the artist.                                                                          |
| artist_description         |           | Short description of the artist.                                                             |
| artist_biography           |           | Biography of the artist.                                                                     |
| artist_mediation_uuid      |           | Name of the image file related to the artist. Is a deterministic UUID based on the file URL. |
| wikidata_id                |           | Wikidata ID (can be null if not matched).                                                    |
| wikipedia_url              |           | URL of the artist Wikipedia page.                                                            |
| wikidata_image_file_url    |           | URL of the image file from Wikidata.                                                         |
| wikidata_image_author      |           | Author of the image from Wikidata.                                                           |
| wikidata_image_license     |           | License of the image from Wikidata.                                                          |
| wikidata_image_license_url |           | URL of the image license from Wikidata.                                                      |
