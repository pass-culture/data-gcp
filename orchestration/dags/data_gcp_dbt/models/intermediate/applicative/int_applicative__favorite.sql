SELECT  
  DATE(datecreated) AS favorite_creation_date,
  datecreated AS favorite_created_at,
  userId AS user_id,
  offerId AS offer_id 
FROM {{ source('raw','applicative_database_favorite') }} 