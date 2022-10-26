SELECT 
  id, 
  created_at,
  updated_at,
  archived,
  name as dashboard_name,
  description as dashboard_description,
  creator_id as dashboard_creator_id,
  collection_id as dashboard_collection_id

FROM public.report_dashboard