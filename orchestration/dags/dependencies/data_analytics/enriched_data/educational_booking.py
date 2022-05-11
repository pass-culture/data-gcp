def define_enriched_educational_booking_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_educational_booking_data AS (
SELECT
    educational_booking.educational_booking_id
    , booking.booking_id
    , booking.stock_id
    , offer.offer_id
    , offer.offer_name
    , offer.offer_subcategoryid
    , offer.venue_id
    , venue.venue_name
    , venue.venue_department_code
    , booking.booking_amount
    , stock.number_of_tickets
    , eple.nom_etablissement
    , eple.code_departement AS school_department_code
    , eple.libelle_academie
    , booking.booking_creation_date
    , educational_booking_status
    , booking.booking_status
    , booking.booking_is_cancelled
    , booking.booking_is_used
    , booking.booking_cancellation_date
    , educational_booking.educational_booking_confirmation_date
    , educational_booking.educational_booking_confirmation_limit_date
    , booking_used_date
    , booking.booking_reimbursement_date
FROM {dataset}.{table_prefix}educational_booking AS educational_booking
INNER JOIN {dataset}.{table_prefix}booking AS booking  ON educational_booking.educational_booking_id  = booking.educational_booking_id
INNER JOIN {dataset}.{table_prefix}stock AS stock  ON stock.stock_id  = booking.stock_id
LEFT JOIN {dataset}.{table_prefix}offer AS offer  ON offer.offer_id = stock.offer_id
INNER JOIN {dataset}.{table_prefix}venue AS venue ON offer.venue_id = venue.venue_id
INNER JOIN {dataset}.{table_prefix}educational_institution AS educational_institution  ON educational_institution.educational_institution_id = educational_booking.educational_booking_educational_institution_id
LEFT JOIN  {dataset}.eple AS eple  ON eple.id_etablissement  = educational_institution.institution_id
 );
    """


def define_enriched_educational_booking_full_query(dataset, table_prefix=""):
    return f"""
        {define_enriched_educational_booking_query(dataset=dataset, table_prefix=table_prefix)}
    """
