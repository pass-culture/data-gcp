SELECT
    CAST("id" AS varchar(255)) AS educational_domain_venue_id,
    CAST("educationalDomainId" AS varchar(255)) AS educational_domain_id,
    CAST("venueId" AS varchar(255)) AS venue_id
FROM educational_domain_venue
