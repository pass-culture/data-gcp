SELECT 
  id,
  created_at,
  updated_at,
  name as card_name,
  description as card_description,
  creator_id as card_creator_id,
  collection_id as card_collection_id
  
FROM public.report_card