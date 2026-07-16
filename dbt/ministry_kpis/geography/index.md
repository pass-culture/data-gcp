## Scope

Main tables:

- `exp_ministry__address`
- `exp_ministry__user_address`
- `exp_ministry__venue_address`
- `exp_ministry__offerer_representation_address`
- `exp_ministry__geo_iris`

## Concepts

These tables provide territorial dimensions for analyses by region, department, QPV, and IRIS.

- User geography: `exp_ministry__user_address` (residence context for beneficiaries).
- Venue geography: `exp_ministry__venue_address` (location context for cultural supply).
- Offerer representation address: administrative location of organizations.
- `exp_ministry__geo_iris`: IRIS referential with geometry.

## Grain and keys

- `exp_ministry__address`: one row per `address_id`.
- `exp_ministry__user_address`: one row per `user_id`.
- `exp_ministry__venue_address`: one row per `venue_id`.
- `exp_ministry__offerer_representation_address`: one row per offerer-address link.
- `exp_ministry__geo_iris`: one row per `iris_internal_id`.

## Common joins

```
SELECT
    ua.user_id,
    ua.user_region_name,
    ua.user_department_code,
    ua.user_qpv_code,
    gi.iris_code,
    gi.iris_label
FROM exp_ministry__user_address AS ua
LEFT JOIN exp_ministry__geo_iris AS gi
    ON gi.iris_internal_id = ua.user_iris_internal_id;
```

## Example indicators (pseudo-SQL)

### Beneficiary and booking concentration by territory

```
SELECT
    ua.user_region_name,
    ua.user_department_code,
    COUNT(DISTINCT ua.user_id) AS total_users,
    COUNT(DISTINCT b.booking_id) AS total_bookings,
    SUM(b.booking_amount) AS total_booking_amount
FROM exp_ministry__user_address AS ua
LEFT JOIN exp_ministry__booking AS b
    ON b.user_id = ua.user_id
GROUP BY ua.user_region_name, ua.user_department_code
ORDER BY total_booking_amount DESC;
```

### QPV share among active users

```
SELECT
    COUNTIF(ua.user_qpv_code IS NOT NULL) AS users_in_qpv,
    COUNT(*) AS total_users,
    SAFE_DIVIDE(COUNTIF(ua.user_qpv_code IS NOT NULL), COUNT(*)) AS qpv_share
FROM exp_ministry__user_address AS ua;
```

### Supply coverage by venue geography

```
SELECT
    va.venue_region_name,
    va.venue_department_code,
    COUNT(DISTINCT va.venue_id) AS total_venues,
    COUNT(DISTINCT o.offer_id) AS total_offers
FROM exp_ministry__venue_address AS va
LEFT JOIN exp_ministry__offer AS o
    ON o.venue_id = va.venue_id
GROUP BY va.venue_region_name, va.venue_department_code
ORDER BY total_offers DESC;
```
