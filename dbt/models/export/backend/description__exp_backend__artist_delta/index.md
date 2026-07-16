# Table: Artist Delta

The `exp_backend__artist_delta` table contains the new artist data that must be synchronized with the backend application. It is an export from the ml_linkage\_\_delta_artist model.

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
| action                     |           | Action to be taken by the backend during synchronization.(can be null if not matched).       |
| comment                    |           | Comment or additional information related to the delta action.(can be null if not matched).  |
