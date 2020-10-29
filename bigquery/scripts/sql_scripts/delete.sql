delete from big_offer where id in (SELECT id
FROM big_offer
ORDER BY random() LIMIT 1);