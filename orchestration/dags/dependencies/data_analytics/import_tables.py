from dependencies.config import APPLICATIVE_EXTERNAL_CONNECTION_ID, GCP_REGION


def define_import_query(
    table, region=GCP_REGION, external_connection_id=APPLICATIVE_EXTERNAL_CONNECTION_ID
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
            CAST("id" AS varchar(255)) AS user_id, "dateCreated" as user_creation_date,
            "departementCode" as user_department_code, "isBeneficiary" as user_is_beneficiary,
            "isAdmin" as user_is_admin, "resetPasswordTokenValidityLimit" as user_reset_password_token_validity_limit,
            "postalCode" as user_postal_code, "needsToFillCulturalSurvey" as user_needs_to_fill_cultural_survey,
            CAST("culturalSurveyId" AS varchar(255)) as user_cultural_survey_id, "civility" as user_civility,
            "activity" as user_activity, "culturalSurveyFilledDate" as user_cultural_survey_filled_date,
            "hasSeenTutorials" as user_has_seen_tutorials, "address" as user_address, "city" as user_city,
            "lastConnectionDate" as user_last_connection_date, "isEmailValidated" as user_is_email_validated,
            "hasAllowedRecommendations" as user_has_allowed_recommendations,
            "suspensionReason" as user_suspension_reason, "isActive" as user_is_active
        FROM public.user
    """
    cloudsql_queries[
        "user_offerer"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), CAST("userId" AS varchar(255)), CAST("offererId" AS varchar(255)),
            CAST("rights" AS varchar(255))
        FROM public.user_offerer
    """
    cloudsql_queries[
        "bank_information"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), CAST("offererId" AS varchar(255)), CAST("venueId" AS varchar(255)),
            CAST("applicationId" AS varchar(255)), "dateModified",
            CAST("status" AS varchar(255))
        FROM public.bank_information
    """
    cloudsql_queries[
        "payment"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "author", "comment", "recipientName", CAST("bookingId" AS varchar(255)),
            "amount", "reimbursementRule", CAST("transactionEndToEndId" AS varchar(255)), "recipientSiren",
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
            CAST("id" AS varchar(255)) as booking_id, "dateCreated" as booking_creation_date,
            CAST("stockId" AS varchar(255)) as stock_id, "quantity" as booking_quantity,
            CAST("userId" AS varchar(255)) as user_id, "amount" as booking_amount,
            "isCancelled" as booking_is_cancelled, "isUsed" as booking_is_used, "dateUsed" as booking_used_date,
            "cancellationDate" as booking_cancellation_date
        FROM public.booking
    """
    cloudsql_queries[
        "offer"
    ] = """
        SELECT
            CAST("idAtProviders" AS varchar(255)) as offer_id_at_providers,
            "dateModifiedAtLastProvider" as offer_modified_at_last_provider_date,
            CAST("id" AS varchar(255)) as offer_id, "dateCreated" as offer_creation_date,
            CAST("productId" AS varchar(255)) as offer_product_id, CAST("venueId" AS varchar(255)) as venue_id,
            CAST("lastProviderId" AS varchar(255)) as offer_last_provider_id, "bookingEmail" as booking_email,
            "isActive" as offer_is_active, "type" as offer_type, "name" as offer_name,
            "description" as offer_description, "conditions" as offer_conditions, "ageMin" as offer_age_min,
            "ageMax" as offer_age_max, "url" as offer_url, "mediaUrls" as offer_media_urls,
            "durationMinutes" as offer_duration_minutes, "isNational" as offer_is_national,
            "extraData" as offer_extra_data, "isDuo" as offer_is_duo, "fieldsUpdated" as offer_fields_updated,
            "withdrawalDetails" as offer_withdrawal_details
        FROM public.offer
    """
    cloudsql_queries[
        "stock"
    ] = """
        SELECT
            CAST("idAtProviders" AS varchar(255)) AS stock_id_at_providers ,
            "dateModifiedAtLastProvider" AS stock_modified_at_last_provider_date,
            CAST("id" AS varchar(255)) AS stock_id, "dateModified" AS stock_modified_date, "price" AS stock_price,
            "quantity" AS stock_quantity, "bookingLimitDatetime" AS stock_booking_limit_date,
            CAST("lastProviderId" AS varchar(255)) AS stock_last_provider_id,
            CAST("offerId" AS varchar(255)) AS offer_id, "isSoftDeleted" AS stock_is_soft_deleted,
            "beginningDatetime" AS stock_beginning_date, "dateCreated" AS stock_creation_date,
            "fieldsUpdated" AS stock_fields_updated
        FROM public.stock
    """
    cloudsql_queries[
        "venue"
    ] = """
        SELECT
            "thumbCount" AS venue_thumb_count, "idAtProviders" AS venue_id_at_providers,
            "dateModifiedAtLastProvider" AS venue_modified_at_last_provider, "address" as venue_address,
            "postalCode" as venue_postal_code, "city" as venue_city, CAST("id" AS varchar(255)) AS venue_id,
            "name" AS venue_name, "siret" AS venue_siret, "departementCode" AS venue_department_code,
            "latitude" AS venue_latitude, "longitude" AS venue_longitude,
            CAST("managingOffererId" AS varchar(255)) AS venue_managing_offerer_id, "bookingEmail" AS venue_booking_email,
            CAST("lastProviderId" AS varchar(255)) AS venue_last_provider_id, "isVirtual" AS venue_is_virtual,
            "comment" AS venue_comment, "publicName" AS venue_public_name,
            "fieldsUpdated" AS venue_fields_updated, CAST("venueTypeId" AS varchar(255)) AS venue_type_id,
            CAST("venueLabelId" AS varchar(255)) AS venue_label_id, "dateCreated" AS venue_creation_date
        FROM public.venue
    """
    cloudsql_queries[
        "offerer"
    ] = """
        SELECT
            "isActive" AS offerer_is_active, "thumbCount" AS offerer_thumb_count,
            CAST("idAtProviders" AS varchar(255)) AS offerer_id_at_providers,
            "dateModifiedAtLastProvider" AS offerer_modified_at_last_provider_date, "address" AS offerer_address,
            "postalCode" AS offerer_postal_code, "city" AS offerer_city, CAST("id" AS varchar(255)) AS offerer_id,
            "dateCreated" AS offerer_creation_date, "name" AS offerer_name,
            "siren" AS offerer_siren, CAST("lastProviderId" AS varchar(255)) AS offerer_last_provider_id,
            "fieldsUpdated" AS offerer_fields_updated
        FROM public.offerer
    """
    cloudsql_queries[
        "provider"
    ] = """
        SELECT
            "isActive", CAST("id" AS varchar(255)), "name", "localClass", "apiKeyGenerationDate",
            "enabledForPro", "requireProviderIdentifier"
        FROM public.provider
    """
    cloudsql_queries[
        "venue_type"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "label"
        FROM public.venue_type
    """
    cloudsql_queries[
        "venue_label"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "label"
        FROM public.venue_label
    """
    cloudsql_queries[
        "favorite"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), CAST("userId" AS varchar(255)), CAST("offerId" AS varchar(255)),
                CAST("mediationId" AS varchar(255)), "dateCreated"
            FROM public.favorite
        """
    cloudsql_queries[
        "iris_venues"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), CAST("irisId" AS varchar(255)), CAST("venueId" AS varchar(255))
            FROM public.iris_venues
        """
    cloudsql_queries[
        "transaction"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), CAST("native_transaction_id" AS varchar(255)),"issued_at",
                CAST("client_addr" AS varchar(255)), CAST("actor_id" AS varchar(255))
            FROM public.transaction
        """
    cloudsql_queries[
        "local_provider_event"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), CAST("providerId" AS varchar(255)),"date",
                CAST("type" AS varchar(255)), "payload"
            FROM public.local_provider_event
        """
    cloudsql_queries[
        "beneficiary_import_status"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), CAST("status" AS varchar(255)), "date", "detail",
                CAST("beneficiaryImportId" AS varchar(255)),  CAST("authorId" AS varchar(255))
            FROM public.beneficiary_import_status
        """
    cloudsql_queries[
        "deposit"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), "amount", CAST("userId" AS varchar(255)), "source", "dateCreated"
            FROM public.deposit
        """
    cloudsql_queries[
        "beneficiary_import"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), CAST("beneficiaryId" AS varchar(255)), CAST("applicationId" AS varchar(255)),
                CAST("sourceId" AS varchar(255)), "source"
            FROM public.beneficiary_import
        """
    cloudsql_queries[
        "mediation"
    ] = """
            SELECT
                "thumbCount",CAST("idAtProviders" AS varchar(255)), "dateModifiedAtLastProvider",CAST("id" AS varchar(255)),
                "dateCreated",CAST("authorId" AS varchar(255)), CAST("lastProviderId" AS varchar(255)),
                CAST("offerId" AS varchar(255)), "credit", "isActive", "fieldsUpdated"
            FROM public.mediation
        """
    cloudsql_queries[
        "iris_france"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), "irisCode",CAST("centroid" AS varchar(255)), CAST("shape" AS varchar(255))
            FROM public.iris_france
        """
    cloudsql_queries[
        "offer_criterion"
    ] = """
            SELECT
                CAST("id" AS varchar(255)),CAST("offerId" AS varchar(255)), CAST("criterionId" AS varchar(255))
            FROM public.offer_criterion
        """
    cloudsql_queries[
        "allocine_pivot"
    ] = """
            SELECT
                CAST("id" AS varchar(255)),"siret",CAST("theaterId" AS varchar(255))
            FROM public.allocine_pivot
        """
    cloudsql_queries[
        "venue_provider"
    ] = """
            SELECT
                "isActive", CAST("id" AS varchar(255)), CAST("idAtProviders" AS varchar(255)),"dateModifiedAtLastProvider",
                CAST("venueId" AS varchar(255)), CAST("providerId" AS varchar(255)), CAST("venueIdAtOfferProvider" AS varchar(255)),
                "lastSyncDate",  CAST("lastProviderId" AS varchar(255)), CAST("syncWorkerId" AS varchar(255)), "fieldsUpdated"
            FROM public.venue_provider
        """
    cloudsql_queries[
        "allocine_venue_provider_price_rule"
    ] = """
            SELECT
                CAST("id" AS varchar(255)), CAST("allocineVenueProviderId" AS varchar(255)),
                CAST("priceRule" AS varchar(255)), "price"
            FROM public.allocine_venue_provider_price_rule
        """
    cloudsql_queries[
        "allocine_venue_provider"
    ] = """
            SELECT
                CAST("id" AS varchar(255)),"isDuo", "quantity"
            FROM public.allocine_venue_provider
        """
    cloudsql_queries[
        "payment_message"
    ] = """
            SELECT
                CAST("id" AS varchar(255)),"name", "checksum"
            FROM public.payment_message
        """
    cloudsql_queries[
        "feature"
    ] = """
            SELECT
                CAST("id" AS varchar(255)),CAST("name" AS varchar(255)), "description", "isActive"
            FROM public.feature
        """
    cloudsql_queries[
        "criterion"
    ] = """
            SELECT
                CAST("id" AS varchar(255)),"name", "description", "scoreDelta", "endDateTime", "startDateTime"
            FROM public.criterion
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
                '{external_connection_id}',
                '{one_line_external_query}'
            );
        """

    # Define default federated query (for tables that do not need specific CAST)
    default_query = f"SELECT * FROM EXTERNAL_QUERY('{external_connection_id}', 'SELECT * FROM {table}');"

    return queries.get(table, default_query)
