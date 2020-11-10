from dependencies.data_analytics.config import GCP_REGION, CLOUDSQL_DATABASE


def define_import_query(table, region=GCP_REGION, cloudsql_database=CLOUDSQL_DATABASE):
    """
        Given a table (from "cloudsql_database" located in "region"), we build and return the federated query that
        selects table content (for import purpose).
        In order to handle type incompatibility between postgresql and BigQuery (eg. UUID and custom types),
        we sometimes have to explicitly select and cast columns.
    """
    # Define select-queries for tables that need a specific CAST
    cloudsql_queries = {}
    cloudsql_queries["user"] = """
        SELECT
            "id", "validationToken", "email", "password", "publicName", "dateCreated", "departementCode",
            "canBookFreeOffers", "isAdmin", "resetPasswordToken", "resetPasswordTokenValidityLimit",
            "firstName", "lastName", "postalCode", "phoneNumber", "dateOfBirth", "needsToFillCulturalSurvey",
            CAST("culturalSurveyId" AS varchar(255)), "civility", "activity", "culturalSurveyFilledDate",
            "hasSeenTutorials", "address", "city", "lastConnectionDate"
        FROM public.user
    """
    cloudsql_queries["user_offerer"] = """
        SELECT
            "id", "userId", "offererId", CAST("rights" AS varchar(255)), "validationToken"
        FROM public.user_offerer
    """
    cloudsql_queries["bank_information"] = """
        SELECT
            "id", "offererId", "venueId", "iban", "bic", "applicationId", "dateModified",
            CAST("status" AS varchar(255))
        FROM public.bank_information
    """
    cloudsql_queries["payment"] = """
        SELECT
            "id", "author", "comment", "recipientName", "iban", "bic", "bookingId", "amount",
            "reimbursementRule", CAST("transactionEndToEndId" AS varchar(255)), "recipientSiren",
            "reimbursementRate", "transactionLabel", "paymentMessageId"
        FROM public.payment
    """
    cloudsql_queries["payment_status"] = """
        SELECT
            "id", "paymentId", "date", CAST("status" AS varchar(255)), "detail"
        FROM public.payment_status
    """

    # Build specific federated queries
    queries = {}
    for external_table, external_query in cloudsql_queries.items():
        one_line_external_query = ' '.join([line.strip() for line in external_query.splitlines()])
        queries[external_table] = f"""
            SELECT * FROM EXTERNAL_QUERY(
                '{region}.{cloudsql_database}',
                '{one_line_external_query}'
            );
        """

    # Define default federated query (for tables that do not need specific CAST)
    default_query = f"SELECT * FROM EXTERNAL_QUERY('{region}.{cloudsql_database}', 'SELECT * FROM {table}');"

    return queries.get(table, default_query)
