# Table: Delta Product Artist Link

The `ml_linkage__delta_product_artist_link` table contains the new product artist link data that must be synchronized with the backend application. It is an export from the ml_preproc\_\_delta_product_artist_link source computed by the artist_linkage DAG.

## Table description

| name             | data_type | description                                                                                 |
| ---------------- | --------- | ------------------------------------------------------------------------------------------- |
| offer_product_id |           | Identifier for the product associated with the offer.                                       |
| artist_id        |           | Unique identifier of the artist.                                                            |
| artist_type      |           | Whether the artist is a performer or an author.                                             |
| action           |           | Action to be taken by the backend during synchronization.(can be null if not matched).      |
| comment          |           | Comment or additional information related to the delta action.(can be null if not matched). |
