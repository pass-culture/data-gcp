-- En dev et en staging, les tests d'intégration reposent sur le fait que :
-- l'user id 2 est dans le groupe A
-- l'user id 1 est dans le groupe B
-- Il faut donc en plus lancer les commandes suivantes en staging :
-- UPDATE ab_testing_<suffix> set groupid = 'A' where userid = '2';
-- UPDATE ab_testing_<suffix> set groupid = 'B' where userid = '1';

-- Il faut donc en plus lancer les commandes suivantes en dev :
-- UPDATE ab_testing_<suffix> set groupid = 'A' where userid = '1';
-- UPDATE ab_testing_<suffix> set groupid = 'B' where userid = '2';

CREATE TABLE IF NOT EXISTS public.ab_testing_202104_v0_v0bis (userId varchar, groupId text);
INSERT INTO public.ab_testing_202104_v0_v0bis (userId, groupId)(
    SELECT DISTINCT ON ("user_id") "user_id", CASE WHEN RANDOM() > 0.5 THEN 'A' ELSE 'B' END AS groupId
    FROM public.booking
);
