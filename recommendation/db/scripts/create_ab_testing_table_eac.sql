-- En dev et en staging, les tests d'intégration reposent sur le fait que :
-- l'user id 2 est dans le groupe A
-- l'user id 1 est dans le groupe B
-- Il faut donc en plus lancer les commandes suivantes en staging :
-- UPDATE ab_testing_ < suffix > set groupid = 'A' where userid = '2';
-- UPDATE ab_testing_ < suffix > set groupid = 'B' where userid = '1';
-- Il faut donc en plus lancer les commandes suivantes en dev :
-- UPDATE ab_testing_ < suffix > set groupid = 'A' where userid = '1';
-- UPDATE ab_testing_ < suffix > set groupid = 'B' where userid = '2';


CREATE TABLE IF NOT EXISTS public.abc_testing_20220322_v1v2_eac (userId varchar, groupId text);
INSERT INTO public.abc_testing_20220322_v1v2_eac (userId, groupId)(
        SELECT user_id AS "user_id",
            CASE
                WHEN base.rand < 0.33 THEN 'A'
                WHEN base.rand >= 0.67 THEN 'C'
                ELSE 'B'
            END AS groupId
        FROM(
                SELECT RANDOM() as rand, user_id
                FROM public.enriched_user
	            WHERE user_deposit_initial_amount < 300
                AND FLOOR(DATE_PART('DAY',user_deposit_creation_date - user_birth_date)/365) < 18) AS base
               );

UPDATE abc_testing_20220322_v1v2_eac set groupid = 'A' where userid = '1';
UPDATE abc_testing_20220322_v1v2_eac set groupid = 'B' where userid = '2';
UPDATE abc_testing_20220322_v1v2_eac set groupid = 'C' where userid = '3';

CREATE INDEX ab_testing_user_id_eac ON public.abc_testing_20220322_v1v2_eac USING btree (userId);
