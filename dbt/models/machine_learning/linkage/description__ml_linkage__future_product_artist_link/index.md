# Table: Future Product Artist Link

The `ml_linkage__future_product_artist_link` table contains a preview of the product/artist link once ingestion of the delta product artist link and delta artist is completed.

This table is used to validate that the future state of the artists and product artist links are correct before synchronizing with the backend application by running dbt tests on it.

## Table description

| name             | data_type | description                                           |
| ---------------- | --------- | ----------------------------------------------------- |
| offer_product_id |           | Identifier for the product associated with the offer. |
| artist_id        |           | Unique identifier of the artist.                      |
| artist_type      |           | Whether the artist is a performer or an author.       |
