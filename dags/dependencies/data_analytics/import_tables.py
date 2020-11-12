from dependencies.data_analytics.config import GCP_REGION, CLOUDSQL_DATABASE


def define_import_query(table, region=GCP_REGION, cloudsql_database=CLOUDSQL_DATABASE):
    queries = {
        "user": f"SELECT * FROM EXTERNAL_QUERY('{region}.{cloudsql_database}', 'SELECT \"id\", \"validationToken\", \"email\", \"password\", \"publicName\", \"dateCreated\", \"departementCode\", \"canBookFreeOffers\", \"isAdmin\", \"resetPasswordToken\", \"resetPasswordTokenValidityLimit\", \"firstName\", \"lastName\", \"postalCode\", \"phoneNumber\", \"dateOfBirth\", \"needsToFillCulturalSurvey\", CAST(\"culturalSurveyId\" AS varchar(255)), \"civility\", \"activity\", \"culturalSurveyFilledDate\", \"hasSeenTutorials\", \"address\", \"city\", \"lastConnectionDate\" FROM public.user');",
        "user_offerer": f"SELECT * FROM EXTERNAL_QUERY('{region}.{cloudsql_database}', 'SELECT \"id\", \"userId\", \"offererId\", CAST(\"rights\" AS varchar(255)), \"validationToken\" FROM public.user_offerer');",
        "bank_information": f"SELECT * FROM EXTERNAL_QUERY('{region}.{cloudsql_database}', 'SELECT \"id\", \"offererId\", \"venueId\", \"iban\", \"bic\", \"applicationId\", \"dateModified\", CAST(\"status\" AS varchar(255)) FROM public.bank_information');",
        "payment": f"SELECT * FROM EXTERNAL_QUERY('{region}.{cloudsql_database}', 'SELECT \"id\", \"author\", \"comment\", \"recipientName\", \"iban\", \"bic\", \"bookingId\", \"amount\", \"reimbursementRule\", CAST(\"transactionEndToEndId\" AS varchar(255)), \"recipientSiren\", \"reimbursementRate\", \"transactionLabel\", \"paymentMessageId\" FROM public.payment');",
    }
    default_query = f"SELECT * FROM EXTERNAL_QUERY('{region}.{cloudsql_database}', 'SELECT * FROM {table}');"

    return queries.get(table, default_query)
