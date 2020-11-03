CREATE VIEW multiplicator_16
AS SELECT 0 n UNION ALL SELECT 1  UNION ALL SELECT 2  UNION ALL 
   SELECT 3   UNION ALL SELECT 4  UNION ALL SELECT 5  UNION ALL
   SELECT 6   UNION ALL SELECT 7  UNION ALL SELECT 8  UNION ALL
   SELECT 9   UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL
   SELECT 12  UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL 
   SELECT 15;

  
CREATE VIEW multiplicator_256
AS SELECT ( ( hi.n * 16 ) + lo.n ) AS n
     FROM multiplicator_16 lo, multiplicator_16 hi;

CREATE VIEW multiplicator_65536
AS SELECT ( ( hi.n * 256 ) + lo.n ) AS n
     FROM multiplicator_256 lo, multiplicator_256 hi;


INSERT INTO target_offer ( "idAtProviders", "dateModifiedAtLastProvider", "dateCreated", "productId", "venueId", "lastProviderId", "bookingEmail", "isActive", "type", "name", description, conditions, "ageMin", "ageMax", url, "mediaUrls", "durationMinutes", "isNational", "extraData", "isDuo", "fieldsUpdated", "withdrawalDetails", lastupdate )
SELECT NULL, "dateModifiedAtLastProvider", "dateCreated", "productId", "venueId", "lastProviderId", "bookingEmail", "isActive", "type", "name", description, conditions, "ageMin", "ageMax", url, "mediaUrls", "durationMinutes", "isNational", "extraData", "isDuo", "fieldsUpdated", "withdrawalDetails", lastupdate
FROM offer o
JOIN multiplicator_65536 m on m.n between 0 and 6666;