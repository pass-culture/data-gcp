WITH venues_to_link AS (
    SELECT
        venue_id,
        venue_longitude,
        venue_latitude
    FROM
        `{{ bigquery_raw_dataset }}.applicative_database_venue` as venue
        JOIN `{{ bigquery_raw_dataset }}.applicative_database_offerer` as offerer ON venue_managing_offerer_id = offerer_id
    WHERE
        venue.venue_is_virtual is false
        AND offerer.offerer_validation_status='VALIDATED'
)
SELECT
    iris_france.id as irisId,
    venue_id as venueId,
    venue_longitude,
    venue_latitude,
FROM
    `{{ bigquery_clean_dataset }}.iris_france` iris_france,
    venues_to_link
WHERE
    ST_CONTAINS(
        iris_france.shape,
        ST_GEOGPOINT(venue_longitude, venue_latitude)
    )
