from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    create_humanize_id_function,
)


def define_collective_offer_booking_information_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE bookings_per_offer AS
            SELECT
                collective_offer_id
                ,COUNT(DISTINCT collective_booking_id) AS collective_booking_cnt
                , COUNT(DISTINCT CASE WHEN collective_booking_status NOT IN ('CANCELLED') THEN collective_booking_id ELSE NULL END) AS collective_booking_no_cancelled_cnt
                , COUNT(DISTINCT CASE WHEN collective_booking_status IN ('USED', 'REIMBURSED') THEN collective_booking_id ELSE NULL END) AS collective_booking_confirm_cnt
            FROM {dataset}.{table_prefix}collective_booking AS collective_booking
            JOIN {dataset}.{table_prefix}collective_stock AS collective_stock ON collective_stock.collective_stock_id = collective_booking.collective_stock_id
            GROUP BY collective_offer_id;
    """


def define_collective_stock_booking_information_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE bookings_per_stock AS
            SELECT
                collective_stock_id
                , COUNT(DISTINCT CASE WHEN collective_booking_status NOT IN ('CANCELLED') THEN collective_booking_id ELSE NULL END) AS collective_booking_stock_no_cancelled_cnt
            FROM {dataset}.{table_prefix}collective_booking AS collective_booking
            GROUP BY collective_stock_id;
    """


def define_enriched_collective_offer_data_query(
    analytics_dataset, clean_dataset, table_prefix=""
):
    return f"""
        CREATE OR REPLACE TABLE {analytics_dataset}.enriched_collective_offer_data AS (
SELECT
    collective_offer.collective_offer_id
    , collective_offer.collective_offer_name
    , collective_offer.venue_id
    , venue.venue_name
    , venue.venue_department_code
    , venue_region.region_name AS venue_region_name
    , academie_dept.libelle_academie AS venue_academie
    , venue.venue_is_virtual
    , venue.venue_managing_offerer_id AS offerer_id
    , offerer.offerer_name
    , collective_offer.collective_offer_creation_date
    , collective_stock.collective_stock_price
    , collective_stock.collective_stock_beginning_date_time
    , collective_stock.collective_stock_booking_limit_date_time
    , collective_stock.collective_stock_number_of_tickets AS number_of_tickets
    , collective_offer.collective_offer_subcategory_id
    , subcategories.category_id AS collective_offer_category_id
    , collective_offer.collective_offer_is_active
   , CASE WHEN collective_offer.collective_offer_id IN (
            SELECT collective_stock.collective_offer_id
                        FROM {analytics_dataset}.{table_prefix}collective_stock AS collective_stock
                        JOIN {analytics_dataset}.{table_prefix}collective_offer AS collective_offer
                        ON collective_stock.collective_offer_id = collective_offer.collective_offer_id AND collective_offer.collective_offer_is_active
                        LEFT JOIN bookings_per_stock ON collective_stock.collective_stock_id = bookings_per_stock.collective_stock_id
                        WHERE (
                        (DATE(collective_stock.collective_stock_booking_limit_date_time) > CURRENT_DATE OR collective_stock.collective_stock_booking_limit_date_time IS NULL)
                        AND (DATE(collective_stock.collective_stock_beginning_date_time) > CURRENT_DATE OR collective_stock.collective_stock_beginning_date_time IS NULL)
                        AND collective_offer.collective_offer_is_active
                        AND (collective_booking_stock_no_cancelled_cnt IS NULL))
                             ) THEN TRUE ELSE FALSE END AS collective_offer_is_bookable
    ,COALESCE(collective_booking_cnt, 0.0) AS collective_booking_cnt
    ,COALESCE(collective_booking_no_cancelled_cnt, 0.0) AS collective_booking_no_cancelled_cnt
    ,COALESCE(collective_booking_confirm_cnt, 0.0) AS collective_booking_confirm_cnt
    , humanize_id(collective_offer.collective_offer_id) AS collective_offer_humanized_id
    ,CONCAT('https://passculture.pro/offre/', humanize_id(collective_offer.collective_offer_id), '/collectif/edition') AS passculture_pro_url
    , FALSE AS offer_is_template
FROM {analytics_dataset}.{table_prefix}collective_offer AS collective_offer
 JOIN {analytics_dataset}.{table_prefix}venue AS venue ON venue.venue_id = collective_offer.venue_id
LEFT JOIN {analytics_dataset}.{table_prefix}collective_stock AS collective_stock ON collective_stock.collective_offer_id = collective_offer.collective_offer_id
 JOIN {analytics_dataset}.{table_prefix}offerer AS offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
LEFT JOIN {analytics_dataset}.subcategories ON subcategories.id = collective_offer.collective_offer_subcategory_id
 JOIN {analytics_dataset}.academie_dept ON academie_dept.code_dpt = venue.venue_department_code
LEFT JOIN {analytics_dataset}.region_department venue_region ON venue_region.num_dep = venue.venue_department_code
LEFT JOIN bookings_per_offer ON bookings_per_offer.collective_offer_id = collective_offer.collective_offer_id

UNION ALL
SELECT
    template.collective_offer_id
    , template.collective_offer_name
    , template.venue_id
    , venue.venue_name
    , venue.venue_department_code
    , venue_region.region_name AS venue_region_name
    , academie_dept.libelle_academie AS venue_academie
    , venue.venue_is_virtual
    , venue.venue_managing_offerer_id AS offerer_id
    , offerer.offerer_name
    , template.collective_offer_creation_date
    , collective_stock.collective_stock_price
    , collective_stock.collective_stock_beginning_date_time
    , collective_stock.collective_stock_booking_limit_date_time
    , collective_stock.collective_stock_number_of_tickets AS number_of_tickets
    , template.collective_offer_subcategory_id
    , subcategories.category_id AS collective_offer_category_id
    , template.collective_offer_is_active
    , FALSE  AS collective_offer_is_bookable
    ,COALESCE(collective_booking_cnt, 0.0) AS collective_booking_cnt
    ,COALESCE(collective_booking_no_cancelled_cnt, 0.0) AS collective_booking_no_cancelled_cnt
    ,COALESCE(collective_booking_confirm_cnt, 0.0) AS collective_booking_confirm_cnt
    , humanize_id(template.collective_offer_id) AS collective_offer_humanized_id
    ,CONCAT('https://passculture.pro/offre/', 'T-',humanize_id(template.collective_offer_id), '/collectif/edition') AS passculture_pro_url
    , TRUE AS offer_is_template
FROM {analytics_dataset}.{table_prefix}collective_offer_template AS template
JOIN {analytics_dataset}.{table_prefix}venue AS venue ON venue.venue_id = template.venue_id
JOIN {analytics_dataset}.{table_prefix}offerer AS offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
LEFT JOIN {analytics_dataset}.subcategories ON subcategories.id = template.collective_offer_subcategory_id
LEFT JOIN {analytics_dataset}.{table_prefix}collective_stock AS collective_stock ON collective_stock.collective_offer_id = template.collective_offer_id
JOIN {analytics_dataset}.academie_dept ON academie_dept.code_dpt = venue.venue_department_code
LEFT JOIN {analytics_dataset}.region_department venue_region ON venue_region.num_dep = venue.venue_department_code
LEFT JOIN bookings_per_offer ON bookings_per_offer.collective_offer_id = template.collective_offer_id

        );
    """


def define_enriched_collective_offer_data_full_query(
    analytics_dataset, clean_dataset, table_prefix=""
):
    return f"""
        {create_humanize_id_function()}
        {define_collective_offer_booking_information_view_query(dataset=analytics_dataset, table_prefix=table_prefix)}
        {define_collective_stock_booking_information_view_query(dataset=analytics_dataset, table_prefix=table_prefix)}
        {define_enriched_collective_offer_data_query(analytics_dataset =analytics_dataset, clean_dataset= clean_dataset,  table_prefix=table_prefix)}
    """
