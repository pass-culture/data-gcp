def create_collective_booking_ranking_view(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE collective_booking_ranking_view AS (
            SELECT
                collective_booking.collective_booking_id,
                rank() OVER (PARTITION BY collective_booking.educational_institution_id ORDER BY collective_booking.collective_booking_creation_date) AS collective_booking_rank
            FROM {dataset}.{table_prefix}collective_booking AS collective_booking);
        """


def define_enriched_collective_booking_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_collective_booking_data AS (
SELECT
    collective_booking.collective_booking_id
    , collective_booking.booking_id
    , collective_booking.collective_stock_id AS collective_stock_id
    , collective_stock.collective_stock_stock_id AS stock_id
    , collective_stock.collective_offer_id AS collective_offer_id
    , collective_offer_offer_id AS offer_id
    , collective_offer.collective_offer_name
    , collective_offer.collective_offer_subcategory_id
    , collective_offer.collective_offer_venue_id AS venue_id
    , venue.venue_name
    , venue.venue_department_code
    , collective_booking.offerer_id AS offerer_id
    , offerer.offerer_name
    , collective_stock.collective_stock_price AS booking_amount
    , collective_stock.collective_stock_number_of_tickets AS number_of_tickets
    , collective_booking.educational_institution_id AS educational_institution_id
    , collective_booking.educational_year_id AS educational_year_id
    , collective_booking.educational_redactor_id AS educational_redactor_id
    , eple.nom_etablissement
    , eple.code_departement AS school_department_code
    , eple.libelle_academie
    , collective_booking.collective_booking_creation_date
    , collective_booking.collective_booking_cancellation_date
    , collective_booking.collective_booking_status
    , collective_booking.collective_booking_cancellation_reason
    , collective_booking.collective_booking_confirmation_date
    , collective_booking.collective_booking_confirmation_limit_date
    , collective_booking.collective_booking_used_date
    , collective_booking.collective_booking_reimbursement_date
    , collective_booking_ranking_view.collective_booking_rank
FROM {dataset}.{table_prefix}collective_booking AS collective_booking
INNER JOIN {dataset}.{table_prefix}collective_stock AS collective_stock  ON collective_stock.collective_stock_id  = collective_booking.collective_stock_id
INNER JOIN {dataset}.{table_prefix}collective_offer AS collective_offer  ON collective_offer.collective_offer_id = collective_stock.collective_offer_id
INNER JOIN {dataset}.{table_prefix}venue AS venue ON collective_booking.venue_id = venue.venue_id
INNER JOIN {dataset}.{table_prefix}offerer AS offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
INNER JOIN {dataset}.{table_prefix}educational_institution AS educational_institution  ON educational_institution.educational_institution_id = collective_booking.educational_institution_id
LEFT JOIN  {dataset}.eple AS eple  ON eple.id_etablissement  = educational_institution.educational_institution_id
LEFT JOIN collective_booking_ranking_view ON collective_booking_ranking_view.collective_booking_id = collective_booking.collective_booking_id

 );
    """


def define_enriched_collective_booking_full_query(dataset, table_prefix=""):
    return f"""
        {create_collective_booking_ranking_view(dataset=dataset, table_prefix=table_prefix)}
        {define_enriched_collective_booking_query(dataset=dataset, table_prefix=table_prefix)}
    """
