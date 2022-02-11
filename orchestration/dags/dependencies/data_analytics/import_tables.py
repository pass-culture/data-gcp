from dependencies.config import APPLICATIVE_EXTERNAL_CONNECTION_ID, GCP_REGION
from datetime import datetime, timedelta
from dateutil import parser


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
            "departementCode" as user_department_code,
            roles[1] as user_role,
            "postalCode" as user_postal_code, "needsToFillCulturalSurvey" as user_needs_to_fill_cultural_survey,
            CAST("culturalSurveyId" AS varchar(255)) as user_cultural_survey_id, "civility" as user_civility,
            "activity" as user_activity, "culturalSurveyFilledDate" as user_cultural_survey_filled_date,
            "hasSeenTutorials" as user_has_seen_tutorials, "address" as user_address, "city" as user_city,
            "lastConnectionDate" as user_last_connection_date, "isEmailValidated" as user_is_email_validated,
            "suspensionReason" as user_suspension_reason, "isActive" as user_is_active,
            "hasSeenProTutorials" as user_has_seen_pro_tutorials, EXTRACT(YEAR FROM AGE("user"."dateOfBirth")) AS user_age,
            "hasCompletedIdCheck" AS user_has_completed_idCheck,
            "phoneValidationStatus" AS user_phone_validation_status,
            "isEmailValidated" AS user_has_validated_email,
            CAST("notificationSubscriptions" -> \\'marketing_push\\' AS BOOLEAN) AS user_has_enabled_marketing_push,
            CAST("notificationSubscriptions" -> \\'marketing_email\\' AS BOOLEAN) AS user_has_enabled_marketing_email,
            "user"."dateOfBirth" AS user_birth_date,
            "user"."subscriptionState" AS user_subscription_state,
            CASE
            WHEN "user"."schoolType" = \\'PUBLIC_SECONDARY_SCHOOL\\' THEN \\'Collège public\\'
            WHEN "user"."schoolType" = \\'PUBLIC_HIGH_SCHOOL\\' THEN \\'Lycée public\\'
            WHEN "user"."schoolType" = \\'PRIVATE_HIGH_SCHOOL\\' THEN \\'Lycée privé\\'
            WHEN "user"."schoolType" = \\'MILITARY_HIGH_SCHOOL\\' THEN \\'Lycée militaire\\'
            WHEN "user"."schoolType" = \\'HOME_OR_REMOTE_SCHOOLING\\' THEN \\'À domicile (CNED, institut de santé, etc.)\\'
            WHEN "user"."schoolType" = \\'AGRICULTURAL_HIGH_SCHOOL\\' THEN \\'Lycée agricole\\'
            WHEN "user"."schoolType" = \\'APPRENTICE_FORMATION_CENTER\\' THEN \\'Centre de formation apprentis\\'
            WHEN "user"."schoolType" = \\'PRIVATE_SECONDARY_SCHOOL\\' THEN \\'Collège privé\\'
            WHEN "user"."schoolType" = \\'NAVAL_HIGH_SCHOOL\\' THEN \\'Lycée maritime\\'
            ELSE "user"."schoolType" END AS user_school_type
        FROM public.user
    """
    cloudsql_queries[
        "user_offerer"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), CAST("userId" AS varchar(255)), CAST("offererId" AS varchar(255)), 
            CAST(CAST("user_offerer"."validationToken" AS varchar(255)) IS NULL AS boolean) AS user_offerer_is_validated
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
        "pricing"
    ] = """
        SELECT
            CAST("id" AS varchar(255))
            ,CAST("status" AS varchar(255))
            ,CAST("bookingId" AS varchar(255))
            ,CAST("businessUnitId" AS varchar(255))
            ,"creationDate"
            ,"valueDate"
            ,"amount"
            ,"standardRule"
            ,CAST("customRuleId" AS varchar(255))
            ,"revenue"
            ,"siret"
        FROM public.pricing
    """
    cloudsql_queries[
        "pricing_line"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), CAST("pricingId" AS varchar(255)), "amount", CAST("category" AS varchar(255))
        FROM public.pricing_line
    """
    cloudsql_queries[
        "pricing_log"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), CAST("pricingId" AS varchar(255)), "timestamp", "statusBefore", "statusAfter", "reason"
        FROM public.pricing_log
    """
    cloudsql_queries[
        "business_unit"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "name","siret",CAST("bankAccountId" AS varchar(255)), "cashflowFrequency"
            , "invoiceFrequency", "status"
        FROM public.business_unit
    """
    cloudsql_queries[
        "cashflow"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "creationDate","status",CAST("bankAccountId" AS varchar(255)), CAST("batchId" AS varchar(255))
            , "amount",CAST("transactionId" AS varchar(255))
        FROM public.cashflow
    """
    cloudsql_queries[
        "cashflow_batch"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), "creationDate", "cutoff"
        FROM public.cashflow_batch
    """
    cloudsql_queries[
        "cashflow_log"
    ] = """
        SELECT
            CAST("id" AS varchar(255)), CAST("cashflowId" AS varchar(255)), "timestamp","statusBefore", "statusAfter"
            ,"details"
        FROM public.cashflow_log
    """
    cloudsql_queries[
        "cashflow_pricing"
    ] = """
        SELECT
            CAST("cashflowId" AS varchar(255)), CAST("pricingId" AS varchar(255))
        FROM public.cashflow_pricing
    """
    cloudsql_queries[
        "booking"
    ] = """
        SELECT
            CAST("id" AS varchar(255)) as booking_id, "dateCreated" as booking_creation_date,
            CAST("stockId" AS varchar(255)) as stock_id, "quantity" as booking_quantity,
            CAST("userId" AS varchar(255)) as user_id, "amount" as booking_amount, CAST("status" AS varchar(255)) AS booking_status,
            "status" = \\'CANCELLED\\' AS booking_is_cancelled, "status" IN (\\'USED\\', \\'REIMBURSED\\') as booking_is_used,
            "dateUsed" as booking_used_date,"cancellationDate" as booking_cancellation_date,
            CAST("cancellationReason" AS VARCHAR) AS booking_cancellation_reason,
            CAST("individualBookingId" AS varchar(255)) as individual_booking_id,
            CAST("educationalBookingId" AS varchar(255)) as educational_booking_id,
            "reimbursementDate" AS booking_reimbursement_date
        FROM public.booking
    """
    # define day before and after execution date
    # we jinja template reference to user the dates around execution date
    DAY_BEFORE_EXECUTION = "{{ yesterday_ds }}"
    DAY_AFTER_EXECUTION = "{{ tomorrow_ds }}"
    cloudsql_queries[
        "offer"
    ] = f"""
        SELECT
            CAST("idAtProvider" AS varchar(255)) as offer_id_at_providers,
            "dateModifiedAtLastProvider" as offer_modified_at_last_provider_date,
            CAST("id" AS varchar(255)) as offer_id, "dateCreated" as offer_creation_date,
            CAST("productId" AS varchar(255)) as offer_product_id, CAST("venueId" AS varchar(255)) as venue_id,
            CAST("lastProviderId" AS varchar(255)) as offer_last_provider_id, "bookingEmail" as booking_email,
            "isActive" as offer_is_active, "name" as offer_name,
            "description" as offer_description, "conditions" as offer_conditions, "ageMin" as offer_age_min,
            "ageMax" as offer_age_max, "url" as offer_url, "mediaUrls" as offer_media_urls,
            "durationMinutes" as offer_duration_minutes, "isNational" as offer_is_national,
            "extraData" as offer_extra_data, "isDuo" as offer_is_duo, "fieldsUpdated" as offer_fields_updated,
            "withdrawalDetails" as offer_withdrawal_details,
            "audioDisabilityCompliant" as offer_audio_disability_compliant,
            "mentalDisabilityCompliant" as offer_mental_disability_compliant,
            "motorDisabilityCompliant" as offer_motor_disability_compliant,
            "visualDisabilityCompliant" as offer_visual_disability_compliant,
            "externalTicketOfficeUrl" as offer_external_ticket_office_url,
            CAST("validation" AS varchar(255)) as offer_validation,
            CAST("subcategoryId" AS varchar(255)) as offer_subcategoryId,
            "dateUpdated" as offer_date_updated,
            "isEducational" AS offer_is_educational
        FROM public.offer
        WHERE "dateUpdated" >= \\'{DAY_BEFORE_EXECUTION}\\'
        AND "dateUpdated" <  \\'{DAY_AFTER_EXECUTION}\\' 
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
            "fieldsUpdated" AS stock_fields_updated,"numberOfTickets" AS number_of_tickets,
            "educationalPriceDetail" AS educational_price_detail
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
            CAST("venueLabelId" AS varchar(255)) AS venue_label_id, "dateCreated" AS venue_creation_date,
            "isPermanent" AS venue_is_permanent, "validationToken" AS venue_validation_token, 
            CAST("businessUnitId" AS varchar(255)) AS business_unit_id
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
            "fieldsUpdated" AS offerer_fields_updated, "validationToken" AS offerer_validation_token
        FROM public.offerer
    """
    cloudsql_queries[
        "provider"
    ] = """
        SELECT
            "isActive", CAST("id" AS varchar(255)), "name", "localClass",
            "enabledForPro"
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
                CAST("id" AS varchar(255)), "amount", CAST("userId" AS varchar(255)), "source", "dateCreated", "expirationDate","type"
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
                "lastSyncDate",  CAST("lastProviderId" AS varchar(255)), "fieldsUpdated"
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
                CAST("id" AS varchar(255)),"name", "description", "endDateTime", "startDateTime"
            FROM public.criterion
        """
    cloudsql_queries[
        "offer_report"
    ] = """
            SELECT
                CAST("id" AS varchar(255)) AS offer_report_id
                ,CAST("userId" AS varchar(255)) AS offer_report_user_id
                ,CAST("offerId" AS varchar(255)) AS offer_report_offer_id
                ,reason AS offer_report_reason
                ,"customReasonContent" AS offer_report_custom_reason_content
                ,"reportedAt" AS offer_report_date
            FROM public.offer_report
        """
    cloudsql_queries[
        "beneficiary_fraud_review"
    ] = """
            SELECT
                CAST("id" AS varchar(255)) AS id
                ,CAST("userId" AS varchar(255)) AS user_id
                ,CAST("authorId" AS varchar(255)) AS author_id
                ,review AS review
                ,"dateReviewed" AS datereviewed
                ,reason AS reason
            FROM public.beneficiary_fraud_review
        """
    cloudsql_queries[
        "beneficiary_fraud_result"
    ] = """
            SELECT
                CAST("id" AS varchar(255)) AS id
                ,CAST("userId" AS varchar(255)) AS user_id
                ,status AS status
                ,"dateCreated" AS datecreated
                ,"dateUpdated" AS dateupdated
            FROM public.beneficiary_fraud_result
        """

    cloudsql_queries[
        "beneficiary_fraud_check"
    ] = r"""
            SELECT id, datecreated, user_id, type, reason, reasonCodes, status, eligibility_type, thirdpartyid
                ,regexp_replace(content, \'"(email|phone|lastName|birthDate|firstName|phoneNumber|reason_code|account_email|last_name|birth_date|first_name|phone_number)": "[^"]*",\' ,\'"\\1":"XXX",\', \'g\') as result_content
                FROM (
                SELECT CAST("id" AS varchar(255)) AS id
                    ,"dateCreated" AS datecreated
                    ,CAST("userId" AS varchar(255)) AS user_id
                    ,type AS type
                    ,"reason" AS reason
                    ,"reasonCodes"[1] AS reasonCodes
                    ,"status" AS status
                    ,"eligibilityType" AS eligibility_type
                    ,"thirdPartyId" AS thirdpartyid
                    ,CAST("resultContent" AS text) as content 
                    FROM public.beneficiary_fraud_check
                    ) AS data
        """

    cloudsql_queries[
        "educational_booking"
    ] = """
            SELECT
                CAST(id AS varchar(255)) AS educational_booking_id
                ,CAST("educationalInstitutionId" AS varchar(255)) AS educational_booking_educational_institution_id
                ,CAST("educationalYearId" AS varchar(255)) AS educational_booking_educational_year_id
                ,CAST("status" AS VARCHAR) AS educational_booking_status
                ,"confirmationDate" AS educational_booking_confirmation_date
                ,"confirmationLimitDate" AS educational_booking_confirmation_limit_date
                ,CAST("educationalRedactorId" AS varchar(255)) AS educational_booking_educational_redactor_id
            FROM educational_booking
        """

    cloudsql_queries[
        "educational_deposit"
    ] = """
            SELECT
                CAST(id AS varchar(255)) AS educational_deposit_id
                ,CAST("educationalInstitutionId" AS varchar(255)) AS educational_deposit_educational_institution_id
                ,CAST("educationalYearId" AS varchar(255)) AS educational_deposit_educational_year_id
                ,amount AS educational_deposit_amount
                ,"dateCreated" AS educational_deposit_creation_date
            FROM educational_deposit
        """

    cloudsql_queries[
        "educational_institution"
    ] = """
            SELECT
            CAST(id AS varchar(255)) AS educational_institution_id
            ,CAST("institutionId" AS varchar(255)) AS educational_institution_institution_id
            FROM educational_institution
        """

    cloudsql_queries[
        "educational_redactor"
    ] = """
            SELECT
                CAST(id AS varchar(255)) AS educational_redactor_id
                ,civility AS educational_redactor_civility
            FROM educational_redactor
        """

    cloudsql_queries[
        "educational_year"
    ] = """
            SELECT
                CAST(id AS varchar(255)) AS educational_year_id
                ,"beginningDate" AS educational_year_beginning_date
                ,"expirationDate" AS educational_year_expiration_date
                ,CAST("adageId" AS varchar(255)) AS educational_year_adage_id
            FROM educational_year
        """
    cloudsql_queries[
        "individual_booking"
    ] = """
            SELECT
                CAST("id" AS varchar(255)) AS individual_booking_id
                ,CAST("userId" AS varchar(255)) AS user_id
                ,CAST("depositId" AS varchar(255)) AS deposit_id
            FROM individual_booking
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
    default_query = f"""SELECT * FROM EXTERNAL_QUERY('{external_connection_id}', 'SELECT * FROM {table}');"""

    return queries.get(table, default_query)


def define_replace_query(columns_to_convert):
    if columns_to_convert == [""]:
        return ""
    else:
        return f"""REPLACE({', '.join([f"DATETIME(timestamp({date_column}),'Europe/Paris') as {date_column}" for date_column in columns_to_convert]) })"""
