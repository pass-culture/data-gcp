select 
uo.offererId as offerer_id, 
u.user_id,
ROW_NUMBER() OVER(PARTITION BY uo.offererId ORDER BY COALESCE(u.user_creation_date, u.user_creation_date)) as user_affiliation_rank,    
u.user_creation_date,
COALESCE(
    CASE
        WHEN u.user_postal_code = '97150' THEN '978'
        WHEN SUBSTRING(u.user_postal_code, 0, 2) = '97' THEN SUBSTRING(u.user_postal_code, 0, 3)
        WHEN SUBSTRING(u.user_postal_code, 0, 2) = '98' THEN SUBSTRING(u.user_postal_code, 0, 3)
        WHEN SUBSTRING(u.user_postal_code, 0, 3) in ('200', '201', '209', '205') THEN '2A'
        WHEN SUBSTRING(u.user_postal_code, 0, 3) in ('202', '206') THEN '2B'
        ELSE SUBSTRING(u.user_postal_code, 0, 2)
        END, 
        user_department_code
    ) AS user_department_code,
u.user_postal_code,
u.user_role,
u.user_address,
u.user_city,
u.user_last_connection_date,
u.user_is_email_validated,
u.user_is_active,
u.user_has_seen_pro_tutorials,
u.user_phone_validation_status,
u.user_has_validated_email,
u.user_has_enabled_marketing_push,
u.user_has_enabled_marketing_email
from `{{ bigquery_raw_dataset }}`.applicative_database_user_offerer as uo
left join  `{{ bigquery_raw_dataset }}`.applicative_database_user as u on uo.userid = u.user_id
where  uo.user_offerer_validation_status = 'VALIDATED'
AND u.user_is_active
order by uo.offererid, u.user_creation_date
