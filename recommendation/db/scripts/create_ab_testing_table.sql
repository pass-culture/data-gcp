-- En dev et en staging, les tests d'intégration reposent sur le fait que :
-- l'user id 2 est dans le groupe A
-- l'user id 1 est dans le groupe B
-- Il faut donc en plus lancer les commandes suivantes en staging :
-- UPDATE ab_testing_ < suffix > set groupid = 'A' where userid = '2';
-- UPDATE ab_testing_ < suffix > set groupid = 'B' where userid = '1';
-- Il faut donc en plus lancer les commandes suivantes en dev :
-- UPDATE ab_testing_ < suffix > set groupid = 'A' where userid = '1';
-- UPDATE ab_testing_ < suffix > set groupid = 'B' where userid = '2';


CREATE TABLE IF NOT EXISTS public.abc_testing_20211029_v1v2 (userId varchar, groupId text);
INSERT INTO public.abc_testing_20211029_v1v2 (userId, groupId)(
        SELECT userid AS "user_id",
            CASE
                WHEN base.rand < 0.33 THEN 'A'
                WHEN base.rand >= 0.67 THEN 'C'
                ELSE 'B'
            END AS groupId
        FROM (
                SELECT RANDOM() as rand,
                    userid
                FROM public.ab_testing_202104_v0_v0bis
            ) AS base
    );

UPDATE abc_testing_20211029_v1v2 set groupid = 'A' where userid = '1';
UPDATE abc_testing_20211029_v1v2 set groupid = 'B' where userid = '2';
UPDATE abc_testing_20211029_v1v2 set groupid = 'C' where userid = '3';

CREATE INDEX ab_testing_user_id ON public.abc_testing_20211029_v1v2 USING btree (userId);
