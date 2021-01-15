from dependencies.data_analytics.config import GCP_REGION, EXTERNAL_CONNECTION_ID


def define_import_query(
    table, region=GCP_REGION, external_connection_id=EXTERNAL_CONNECTION_ID
):
    """
    Given a table (from "external_connection_id" located in "region"), we build and return the federated query that
    selects table content (for import purpose).
    In order to handle type incompatibility between postgresql and BigQuery (eg. UUID and custom types),
    we sometimes have to explicitly select and cast columns.
    """
    # Define select-queries for tables that need a specific CAST and ID for metabase
    cloudsql_queries = {}
    cloudsql_queries[
        "user"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "validationToken", "email", "password", "publicName", "dateCreated", "departementCode",
            "canBookFreeOffers", "isAdmin", "resetPasswordToken", "resetPasswordTokenValidityLimit",
            "firstName", "lastName", "postalCode", "phoneNumber", "dateOfBirth", "needsToFillCulturalSurvey",
            CAST("culturalSurveyId" AS varchar(255)), "civility", "activity", "culturalSurveyFilledDate",
            "hasSeenTutorials", "address", "city", "lastConnectionDate"
        FROM public.user
    """
    cloudsql_queries[
        "user_offerer"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), CAST("userId" AS varchar(255)), CAST("offererId" AS varchar(255)), CAST("rights" AS varchar(255)), "validationToken"
        FROM public.user_offerer
    """
    cloudsql_queries[
        "bank_information"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), CAST("offererId" AS varchar(255)), CAST("venueId" AS varchar(255)), "iban", "bic", CAST("applicationId" AS varchar(255)), "dateModified",
            CAST("status" AS varchar(255))
        FROM public.bank_information
    """
    cloudsql_queries[
        "payment"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "author", "comment", "recipientName", "iban", "bic", CAST("bookingId" AS varchar(255)), "amount",
            "reimbursementRule", CAST("transactionEndToEndId" AS varchar(255)), "recipientSiren",
            "reimbursementRate", "transactionLabel", "paymentMessageId"
        FROM public.payment
    """
    cloudsql_queries[
        "payment_status"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), CAST("paymentId" AS varchar(255)), "date", CAST("status" AS varchar(255)), "detail"
        FROM public.payment_status
    """
    cloudsql_queries[
        "booking"
    ] = """
        SELECT
            "isActive", CAST("id" AS varchar(255)), "dateCreated", CAST("recommendationId" AS varchar(255)), CAST("stockId" AS varchar(255)), "quantity", 
            "token", CAST("userId" AS varchar(255)), "amount", "isCancelled", "isUsed", "dateUsed", "cancellationDate" ,"lastupdate"
        FROM public.booking
    """
    cloudsql_queries[
        "offer"
    ] = """
        SELECT
            CAST("idAtProviders" AS varchar(255)), "dateModifiedAtLastProvider", CAST("id" AS varchar(255)), "dateCreated", CAST("productId" AS varchar(255)),
            CAST("venueId" AS varchar(255)), CAST("lastProviderId" AS varchar(255)), "bookingEmail", "isActive", "type", "name", "description",
            "conditions", "ageMin", "ageMax", "url", "mediaUrls", "durationMinutes", "isNational" ,
            "extraData", "isDuo", "fieldsUpdated", "withdrawalDetails", "lastupdate"
        FROM public.offer
    """
    cloudsql_queries[
        "stock"
    ] = """
        SELECT
            CAST("idAtProviders" AS varchar(255)), "dateModifiedAtLastProvider", CAST("id" AS varchar(255)), "dateModified", "price", "quantity",
            "bookingLimitDatetime", CAST("lastProviderId" AS varchar(255)), CAST("offerId" AS varchar(255)), "isSoftDeleted", "beginningDatetime",
            "dateCreated", "fieldsUpdated", "hasBeenMigrated", "lastupdate" 
        FROM public.stock
    """
    cloudsql_queries[
        "venue"
    ] = """
        SELECT
            "thumbCount", "firstThumbDominantColor", "idAtProviders", "dateModifiedAtLastProvider", "address", "postalCode",
            "city", CAST("id" AS varchar(255)) , "name", "siret", "departementCode", "latitude", "longitude", CAST("managingOffererId" AS varchar(255)), "bookingEmail",
            CAST("lastProviderId" AS varchar(255)), "isVirtual", "comment", "validationToken", "publicName", "fieldsUpdated", CAST("venueTypeId" AS varchar(255)),
            CAST("venueLabelId" AS varchar(255)), "dateCreated", "lastupdate"  
        FROM public.venue
    """
    cloudsql_queries[
        "offerer"
    ] = """
        SELECT
            "isActive", "thumbCount", "firstThumbDominantColor", CAST("idAtProviders" AS varchar(255)), "dateModifiedAtLastProvider", "address",
            "postalCode", "city", "validationToken", CAST("id" AS varchar(255)), "dateCreated", "name", "siren", CAST("lastProviderId" AS varchar(255)), "fieldsUpdated","lastupdate"  
        FROM public.offerer
    """
    cloudsql_queries[
        "provider"
    ] = """
        SELECT
            "isActive", CAST("id" AS varchar(255)), "name", "localClass", "apiKey", "apiKeyGenerationDate", "enabledForPro", "requireProviderIdentifier","lastupdate"
        FROM public.provider
    """
    cloudsql_queries[
        "venue_type"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "label", "lastupdate" 
        FROM public.venue_type
    """
    cloudsql_queries[
        "venue_label"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "label", "lastupdate" 
        FROM public.venue_label
    """
    cloudsql_queries[
        "favorite"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), CAST("userId" AS varchar(255)), CAST("offerId" AS varchar(255)), CAST("mediationId" AS varchar(255)), "lastupdate"
            FROM public.favorite
        """

    # Build specific federated queries
    queries = {}
    for external_table, external_query in cloudsql_queries.items():
        one_line_external_query = " ".join(
            [line.strip() for line in external_query.splitlines()]
        )
        queries[
            external_table
        ] = f"""
            SELECT * FROM EXTERNAL_QUERY(
                '{region}.{external_connection_id}',
                '{one_line_external_query}'
            );
        """

    # Define default federated query (for tables that do not need specific CAST)
    default_query = f"SELECT * FROM EXTERNAL_QUERY('{region}.{external_connection_id}', 'SELECT * FROM {table}');"

    return queries.get(table, default_query)
