CREATE TABLE IF NOT EXISTS public.ab_testing_202104_v0_v0bis (userId varchar, groupId text);
INSERT INTO public.ab_testing_202104_v0_v0bis (userId, groupId)(
    SELECT DISTINCT ON ("user_id") "user_id", CASE WHEN RANDOM() > 0.5 THEN 'A' ELSE 'B' END AS groupId
    FROM public.booking
);
