UPDATE target_offer SET "lastupdate" = NOW() WHERE id IN (SELECT id
FROM target_offer
ORDER BY random() LIMIT 10000000); 
