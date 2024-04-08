SELECT
  CASE
      WHEN LENGTH(cast(gtl_id as string) ) = 7 THEN CONCAT('0', cast(gtl_id as string) )
      ELSE cast(gtl_id as string) 
    END AS gtl_id,
    UPPER(gtl_type) as gtl_type,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4
FROM {{ source('raw','applicative_database_titelive_gtl_mapping') }}