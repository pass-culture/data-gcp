DELETE FROM target_offer WHERE id IN (SELECT id
FROM target_offer
ORDER BY random() LIMIT 1000000);