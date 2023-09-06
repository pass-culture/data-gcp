SELECT
    CAST("id" AS varchar(255)) AS venue_criterion_id,
    CAST("venueId" AS varchar(255)) AS venue_id,
    CAST("criterionId" AS varchar(255)) AS criterion_id
FROM venue_criterion
