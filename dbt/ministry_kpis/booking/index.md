## Scope

Main table:

- `exp_ministry__booking`

Core linked tables:

- `exp_ministry__user`
- `exp_ministry__offer`
- `exp_ministry__stock`
- `exp_ministry__user_address`

## Concepts

A booking is one transaction line identified by `booking_id`.

Main business fields:

- `booking_creation_date`
- `booking_amount`
- `booking_quantity`
- `booking_status`
- `booking_cancellation_date`, `booking_cancellation_reason`

Main foreign keys:

- `user_id`, `deposit_id`, `stock_id`, `offer_id`, `venue_id`, `offerer_id`

## Grain and keys

- One row per `booking_id`.
- Time analyses are usually done with `DATE_TRUNC(booking_creation_date, MONTH)`.

## Example indicators (pseudo-SQL)

### Booking volume and amount by month

```
SELECT
    DATE_TRUNC(booking_creation_date, MONTH) AS partition_month,
    COUNT(DISTINCT booking_id) AS total_bookings,
    SUM(booking_amount) AS total_booking_amount
FROM exp_ministry__booking
WHERE booking_status NOT IN ('CANCELLED')
GROUP BY partition_month
ORDER BY partition_month;
```

### Booking amount by region and department

```
SELECT
    DATE_TRUNC(b.booking_creation_date, MONTH) AS partition_month,
    ua.user_region_name,
    ua.user_department_code,
    COUNT(DISTINCT b.booking_id) AS total_bookings,
    SUM(b.booking_amount) AS total_booking_amount
FROM exp_ministry__booking AS b
LEFT JOIN exp_ministry__user_address AS ua
    ON ua.user_id = b.user_id
WHERE b.booking_status NOT IN ('CANCELLED')
GROUP BY partition_month, ua.user_region_name, ua.user_department_code
ORDER BY partition_month;
```

### Mean basket and cancellation rate

```
SELECT
    DATE_TRUNC(booking_creation_date, MONTH) AS partition_month,
    AVG(booking_amount) AS avg_booking_amount,
    SAFE_DIVIDE(
        COUNTIF(booking_status = 'CANCELLED'),
        COUNT(*)
    ) AS cancellation_rate
FROM exp_ministry__booking
GROUP BY partition_month
ORDER BY partition_month;
```
