do $$ begin execute format('CREATE TABLE IF NOT EXISTS public.ab_testing_%s (userId bigint, groupId text)', to_char(CURRENT_DATE, 'YYYYMMDD')); end; $$;
do $$ begin execute format('INSERT INTO public."ab_testing_%s" (userId, groupId) (SELECT DISTINCT "userId", CASE WHEN RANDOM() > 0.5 THEN ''A'' ELSE ''B'' END AS groupId from public.booking)', to_char(CURRENT_DATE, 'YYYYMMDD')); end; $$;
