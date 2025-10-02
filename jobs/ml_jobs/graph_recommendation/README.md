# Graph Recommendation

## Resources

###  Queries to get the training data

Get products for books with metadata and artists.

```sql
    WITH
        offers AS (
            SELECT
                offer_id,
                offer_product_id,
                item_id,
                offer_name,
                offer_subcategory_id,
                rayon
            FROM
                `passculture-data-prod.analytics_prod.global_offer`
            WHERE
                offer_category_id = "LIVRE" ),
        offers_with_extended_metadata AS (
            SELECT
                offers.offer_id,
                offers.offer_product_id,
                offers.item_id,
                offers.offer_name,
                offers.offer_subcategory_id,
                offers.rayon,
                offer_metadata.gtl_type,
                offer_metadata.gtl_label_level_1,
                offer_metadata.gtl_label_level_2,
                offer_metadata.gtl_label_level_3,
                offer_metadata.gtl_label_level_4,
                offer_metadata.author,
                artist_link.artist_id,
                artist_link.artist_type,
                artist.artist_name
            FROM
                offers
            LEFT JOIN
                `passculture-data-prod.analytics_prod.global_offer_metadata` offer_metadata
            USING
                (offer_id)
            LEFT JOIN
                `passculture-data-prod.raw_prod.applicative_database_product_artist_link` artist_link
            ON
                offers.offer_product_id = CAST(artist_link.offer_product_id AS STRING)
            LEFT JOIN
                `passculture-data-prod.raw_prod.applicative_database_artist` artist
            USING
                (artist_id)),
        offer_with_score AS (
            SELECT
                *,
                CAST(gtl_label_level_1 IS NOT NULL AS int) AS has_gtl
            FROM
                offers_with_extended_metadata)
    SELECT
        offer_id AS example_offer_id,
        offer_product_id,
        item_id,
        offer_name,
        offer_subcategory_id,
        rayon,
        gtl_type,
        gtl_label_level_1,
        gtl_label_level_2,
        gtl_label_level_3,
        gtl_label_level_4,
        author,
        artist_id,
        artist_name,
        artist_type
    FROM
        offer_with_score
        QUALIFY
        ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY has_gtl DESC ) = 1;
```

* The data is exported at this GCS location : `gs://data-bucket-prod/sandbox_prod/lmontier/graph_recommendation/book_item_for_graph_recommendation.parquet`
  * To download it:

    ```bash
    gsutil cp gs://data-bucket-prod/sandbox_prod/lmontier/graph_recommendation/book_item_for_graph_recommendation.parquet ./data/
    ```
