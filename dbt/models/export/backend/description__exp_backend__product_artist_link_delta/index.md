# Table: Product Artist Link Delta

The `exp_backend__product_artist_link_delta` table contains the new product artist link data that must be synchronized with the backend application. It is an export from the ml_linkage\_\_delta_product_artist_link model.

## Table description

| name             | data_type | description                                                                                 |
| ---------------- | --------- | ------------------------------------------------------------------------------------------- |
| offer_product_id |           | Identifier for the product associated with the offer.                                       |
| artist_id        |           | Unique identifier of the artist.                                                            |
| artist_type      |           | Whether the artist is a performer or an author.                                             |
| action           |           | Action to be taken by the backend during synchronization.(can be null if not matched).      |
| comment          |           | Comment or additional information related to the delta action.(can be null if not matched). |
