SELECT * FROM public.past_recommended_offers WHERE date <= '{{ params.yesterday }}';