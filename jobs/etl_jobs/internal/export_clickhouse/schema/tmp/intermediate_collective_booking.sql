CREATE TABLE IF NOT EXISTS {{ dataset }}.{{ tmp_table_name }} ON cluster default
    ENGINE = MergeTree
    PARTITION BY update_date
    ORDER BY (venue_id, offerer_id, collective_booking_status, collective_offer_id)
    SETTINGS storage_policy='gcs_main'

AS
    SELECT
        '{{ date }}' as update_date,
        cast(venue_id as String) as venue_id,
        cast(offerer_id as String) as offerer_id,
        cast(collective_offer_id as String) as collective_offer_id,
        cast(date(creation_date) as String) as creation_date,
        cast(date(used_date) as Nullable(String)) as used_date,
        cast(date(reimbursement_date) as Nullable(String)) as reimbursement_date,
        cast(collective_booking_status as String) as collective_booking_status,
        cast(educational_institution_id as String) as educational_institution_id,
        cast(number_of_tickets as UInt64) as number_of_tickets,
        cast(booking_amount as Float64) as booking_amount
    FROM s3(
        gcs_credentials,
        url='{{ bucket_path }}'
)
