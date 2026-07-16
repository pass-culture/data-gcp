## Scope

Main tables:

- `exp_ministry__offerer`
- `exp_ministry__venue`

## Concepts

- An offerer is a cultural organization (`offerer_id`, legal identity fields).
- A venue is an operational place managed by an offerer (`venue_id`, `offerer_id`).
- The pair supports network analyses: organization footprint, validation status, and activity.

## Grain and keys

- `exp_ministry__offerer`: one row per `offerer_id`.
- `exp_ministry__venue`: one row per `venue_id`.
- Main relationship: `venue.offerer_id = offerer.offerer_id`.

## Common joins

```
SELECT
    o.offerer_id,
    o.offerer_name,
    o.offerer_siren,
    o.offerer_validation_status,
    v.venue_id,
    v.venue_name,
    v.venue_type_label
FROM exp_ministry__offerer AS o
LEFT JOIN exp_ministry__venue AS v
    ON v.offerer_id = o.offerer_id;
```

## Example indicators (pseudo-SQL)

### Offerers and venue footprint

```
SELECT
    o.offerer_validation_status,
    COUNT(DISTINCT o.offerer_id) AS total_offerers,
    COUNT(DISTINCT v.venue_id) AS total_venues
FROM exp_ministry__offerer AS o
LEFT JOIN exp_ministry__venue AS v
    ON v.offerer_id = o.offerer_id
GROUP BY o.offerer_validation_status;
```

### Venue activity proxy from bookings

```
SELECT
    v.venue_id,
    v.venue_name,
    COUNT(DISTINCT b.booking_id) AS total_bookings,
    SUM(b.booking_amount) AS total_booking_amount
FROM exp_ministry__venue AS v
LEFT JOIN exp_ministry__booking AS b
    ON b.venue_id = v.venue_id
GROUP BY v.venue_id, v.venue_name
ORDER BY total_booking_amount DESC;
```
