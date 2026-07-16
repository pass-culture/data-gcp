## Scope

Main tables:

- `exp_ministry__offer`
- `exp_ministry__stock`
- `exp_ministry__offer_metadata`

## Concepts

- `exp_ministry__offer` describes the cultural proposition (catalog layer).
- `exp_ministry__stock` describes sellable availability and price (commercial layer).
- `exp_ministry__offer_metadata` provides optional content enrichment (movie/book/taxonomy fields).

## Grain and keys

- `exp_ministry__offer`: one row per `offer_id`.
- `exp_ministry__stock`: one row per `stock_id`.
- `exp_ministry__offer_metadata`: one row per `offer_id` when metadata exists.
- Main join key: `offer_id`.

## Common joins

```
SELECT
    o.offer_id,
    o.offer_category,
    o.offer_subcategory,
    o.offer_is_bookable,
    o.offer_is_active,
    s.stock_id,
    s.stock_price,
    s.stock_quantity,
    s.stock_booking_limit_date
FROM exp_ministry__offer AS o
LEFT JOIN exp_ministry__stock AS s
    ON s.offer_id = o.offer_id;
```

## Example indicators (pseudo-SQL)

### Active supply by category

```
SELECT
    o.offer_category,
    COUNT(DISTINCT o.offer_id) AS total_offers,
    COUNT(DISTINCT s.stock_id) AS total_stocks
FROM exp_ministry__offer AS o
LEFT JOIN exp_ministry__stock AS s
    ON s.offer_id = o.offer_id
WHERE o.offer_is_active = TRUE
GROUP BY o.offer_category
ORDER BY total_offers DESC;
```

### Price distribution by category

```
SELECT
    o.offer_category,
    AVG(s.stock_price) AS avg_price,
    APPROX_QUANTILES(s.stock_price, 4)[OFFSET(2)] AS median_price
FROM exp_ministry__offer AS o
JOIN exp_ministry__stock AS s
    ON s.offer_id = o.offer_id
WHERE s.stock_price IS NOT NULL
GROUP BY o.offer_category;
```

### Metadata coverage

```
SELECT
    o.offer_category,
    COUNT(DISTINCT o.offer_id) AS total_offers,
    COUNT(DISTINCT om.offer_id) AS offers_with_metadata,
    SAFE_DIVIDE(COUNT(DISTINCT om.offer_id), COUNT(DISTINCT o.offer_id)) AS metadata_coverage_rate
FROM exp_ministry__offer AS o
LEFT JOIN exp_ministry__offer_metadata AS om
    ON om.offer_id = o.offer_id
GROUP BY o.offer_category;
```
