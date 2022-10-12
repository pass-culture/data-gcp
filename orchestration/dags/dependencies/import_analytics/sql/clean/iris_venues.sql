WITH venues_to_link AS (
        SELECT 
            venue_id, 
            venue_longitude,
            venue_latitude
        FROM `{{ bigquery_clean_dataset }}.applicative_database_venue` as venue
        JOIN  `{{ bigquery_clean_dataset }}.applicative_database_offerer` as offerer ON venue_managing_offerer_id=offerer_id
        WHERE venue_is_virtual is false
        AND venue_validation_token is null
        AND offerer_validation_token is null
    )
SELECT 
    iris_france.id as irisId, 
    venue_id as venueId ,
    venue_longitude,
    venue_latitude,
FROM `{{ bigquery_clean_dataset }}.iris_france` iris_france, venues_to_link
WHERE ST_DISTANCE(centroid, ST_GEOGPOINT(venue_longitude, venue_latitude)) < {{ params.iris_distance }}