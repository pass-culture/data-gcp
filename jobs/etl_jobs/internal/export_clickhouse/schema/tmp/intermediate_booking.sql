CREATE TABLE IF NOT EXISTS {{ dataset }}.{{ tmp_table_name }} ON cluster default
    ENGINE = MergeTree
    PARTITION BY update_date
    ORDER BY (offerer_id, booking_status, offer_id)
    SETTINGS storage_policy='gcs_main'
    
AS 
    SELECT
        cast(update_date as String) as update_date,
        cast(offerer_id as String) as offerer_id,
        cast(offer_id as String) as offer_id,
        cast(date(creation_date) as String) as creation_date,
        cast(date(used_date) as Nullable(String)) as used_date,
        cast(booking_status as String) as booking_status,
        cast(deposit_type as String) as deposit_type,
        cast(booking_quantity as UInt64) as booking_quantity,
        cast(booking_amount as Float64) as booking_amount
    FROM s3('{{ bucket_path }}','{{ access_key_id }}','{{ secret_access_key }}')