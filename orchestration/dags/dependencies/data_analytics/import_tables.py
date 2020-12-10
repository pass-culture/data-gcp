from dependencies.data_analytics.config import GCP_REGION, CLOUDSQL_DATABASE


def define_import_query(table, region=GCP_REGION, cloudsql_database=CLOUDSQL_DATABASE):
    """
    Given a table (from "cloudsql_database" located in "region"), we build and return the federated query that
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
            CAST("id" AS varchar(255)), "userId", "offererId", CAST("rights" AS varchar(255)), "validationToken"
        FROM public.user_offerer
    """
    cloudsql_queries[
        "bank_information"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "offererId", "venueId", "iban", "bic", "applicationId", "dateModified",
            CAST("status" AS varchar(255))
        FROM public.bank_information
    """
    cloudsql_queries[
        "payment"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "author", "comment", "recipientName", "iban", "bic", "bookingId", "amount",
            "reimbursementRule", CAST("transactionEndToEndId" AS varchar(255)), "recipientSiren",
            "reimbursementRate", "transactionLabel", "paymentMessageId"
        FROM public.payment
    """
    cloudsql_queries[
        "payment_status"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "paymentId", "date", CAST("status" AS varchar(255)), "detail"
        FROM public.payment_status
    """
    cloudsql_queries[
        "booking"
    ] = """
        SELECT
            "isActive", CAST("id" AS varchar(255)), "dateCreated", "recommendationId", "stockId", "quantity", 
            "token", "userId", "amount", "isCancelled", "isUsed", "dateUsed", "cancellationDate" ,"lastupdate"
        FROM public.booking
    """
    cloudsql_queries[
        "offer"
    ] = """
        SELECT
            "idAtProviders", "dateModifiedAtLastProvider", CAST("id" AS varchar(255)), "dateCreated", "productId",
            "venueId", "lastProviderId", "bookingEmail", "isActive", "type",	"name",	"description",
            "conditions", "ageMin", "ageMax", "url", "mediaUrls", "durationMinutes", "isNational" ,
            "extraData", "isDuo", "fieldsUpdated", "withdrawalDetails", "lastupdate"
        FROM public.offer
    """
    cloudsql_queries[
        "stock"
    ] = """
        SELECT
            "idAtProviders", "dateModifiedAtLastProvider", CAST("id" AS varchar(255)), "dateModified", "price", "quantity",
            "bookingLimitDatetime", "lastProviderId", "offerId", "isSoftDeleted", "beginningDatetime",
            "dateCreated", "fieldsUpdated", "hasBeenMigrated", "lastupdate" 
        FROM public.stock
    """
    cloudsql_queries[
        "venue"
    ] = """
        SELECT
            "thumbCount", "firstThumbDominantColor", "idAtProviders", "dateModifiedAtLastProvider", "address", "postalCode",
            "city", CAST("id" AS varchar(255)) , "name", "siret", "departementCode", "latitude", "longitude", "managingOffererId", "bookingEmail",
            "lastProviderId", "isVirtual", "comment", "validationToken", "publicName", "fieldsUpdated", "venueTypeId",
            "venueLabelId", "dateCreated", "lastupdate"  
        FROM public.venue
    """
    cloudsql_queries[
        "offerer"
    ] = """
        SELECT
            "isActive", "thumbCount", "firstThumbDominantColor", "idAtProviders", "dateModifiedAtLastProvider", "address",
            "postalCode", "city", "validationToken", CAST("id" AS varchar(255)), "dateCreated", "name", "siren", "lastProviderId", "fieldsUpdated","lastupdate"  
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
                '{region}.{cloudsql_database}',
                '{one_line_external_query}'
            );
        """

    # Define default federated query (for tables that do not need specific CAST)
    default_query = f"SELECT * FROM EXTERNAL_QUERY('{region}.{cloudsql_database}', 'SELECT * FROM {table}');"

    return queries.get(table, default_query)
