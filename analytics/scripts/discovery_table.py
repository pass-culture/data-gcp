from google.cloud import bigquery


def _create_function_offer_has_at_least_one_bookable_stock(client) -> str:
    function_name = "public.offer_has_at_least_one_bookable_stock"
    query_job = client.query(
        f"""
        CREATE OR REPLACE FUNCTION {function_name}(offer_id INT64)
         AS ((
            SELECT MAX(1)
            FROM public.stock
            WHERE stock.offerId = offer_id
                AND stock.isSoftDeleted = FALSE
                AND (stock.beginningDatetime > current_datetime()
                    OR stock.beginningDatetime IS NULL)
                AND (stock.bookingLimitDatetime > current_datetime()
                    OR stock.bookingLimitDatetime IS NULL)
                AND (stock.quantity IS NULL
                    OR (
                        SELECT GREATEST(stock.quantity - COALESCE(SUM(booking.quantity), 0), 0)
                        FROM public.booking
                        WHERE booking.stockId = stock.id
                            AND booking.isCancelled = FALSE
                    ) > 0
                )
      ));
    """
    )
    query_job.result()
    return function_name


def _create_function_offer_has_at_least_one_active_mediation(client) -> str:
    function_name = "public.offer_has_at_least_one_active_mediation"
    query_job = client.query(
        f"""
        CREATE OR REPLACE FUNCTION {function_name}(offer_id INT64)
        AS ((
            SELECT MAX(1)
            FROM public.mediation
            WHERE mediation.offerId = offer_id
                AND mediation.isActive ));
    """
    )
    query_job.result()
    return function_name


def _create_function_get_active_offers_ids(client) -> str:
    function_name = "public.get_active_offers_ids"
    offer_has_at_least_one_active_mediation = (
        _create_function_offer_has_at_least_one_active_mediation(client)
    )
    offer_has_at_least_one_bookable_stock = (
        _create_function_offer_has_at_least_one_bookable_stock(client)
    )

    query_job = client.query(
        f"""
        CREATE OR REPLACE FUNCTION {function_name} ()
        AS
        ((
            ARRAY(SELECT DISTINCT offer.id
            FROM public.offer
            JOIN public.venue ON offer.venueId = venue.id
            JOIN public.offerer ON offerer.id = venue.managingOffererId
            WHERE offer.isActive = TRUE
                AND venue.validationToken IS NULL
                AND (SELECT {offer_has_at_least_one_active_mediation}(offer.id)) = 1
                AND (SELECT {offer_has_at_least_one_bookable_stock}(offer.id)) = 1
                AND offerer.isActive = TRUE
                AND offerer.validationToken IS NULL
                AND offer.type != 'ThingType.ACTIVATION'
                AND offer.type != 'EventType.ACTIVATION'
        )));
    """
    )
    query_job.result()
    return function_name


def create_discovery_table(client, name: str) -> None:
    get_active_offers_ids = _create_function_get_active_offers_ids(client)
    query_job = client.query(
        f"""
        CREATE TABLE IF NOT EXISTS {name} AS
            (SELECT 
                offer.id AS id,
                offer.venueId AS venueId,
                offer.type AS type,
                offer.name AS name,
                offer.url AS url,
                offer.isNational AS isNational,
            FROM public.offer
            WHERE offer.id IN UNNEST((SELECT {get_active_offers_ids}())))
    """
    )
    query_job.result()


if __name__ == "__main__":
    client = bigquery.Client(project="pass-culture-app-projet-test")
    create_discovery_table(client, "public.discovery_table")
