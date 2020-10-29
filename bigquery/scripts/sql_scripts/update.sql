UPDATE big_offer SET "dateModifiedAtLastProvider" = NOW() WHERE id IN (SELECT id
FROM big_offer
ORDER BY random() LIMIT 3); 