# Table: Future Product Artist Link

The `ml_linkage__future_product_artist_link` table contains a preview of the product/artist link once ingestion of the delta product artist link and delta artist is completed.

This table is used to validate that the future state of the artists and product artist links are correct before synchronizing with the backend application by running dbt tests on it.

## Table description

| name             | data_type | description                                                                                                                                                                                                                                                                                                                                                                                                 |
| ---------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| offer_product_id |           | Identifier of the product (`mrt_global__product.product_id`) the offer is attached to; NULL when no product is attached. Almost only synchronized offers (`is_synchronised`) on books, CDs, vinyls and movies carry one. An offer with an EAN but no `offer_product_id` is not attached to a product, which does not mean the product does not exist: look the EAN up in `mrt_global__product.product_ean`. |
| artist_id        |           | Unique identifier of the artist.                                                                                                                                                                                                                                                                                                                                                                            |
| artist_type      |           | Whether the artist is a performer or an author.                                                                                                                                                                                                                                                                                                                                                             |
