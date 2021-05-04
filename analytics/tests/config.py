import os

TEST_DATASET = f"test_{os.uname().nodename.replace('-', '_').replace('.', '_')}"  # Temporary => find a better way to make dataset unique
GCP_REGION = "europe-west1"
GCP_PROJECT = "passculture-data-ehp"
TEST_TABLE_PREFIX = ""
BIGQUERY_SCHEMAS = {
    "booking": {
        "booking_is_active": "BOOLEAN",
        "booking_id": "STRING",
        "booking_creation_date": "DATETIME",
        "recommendation_id": "STRING",
        "stock_id": "STRING",
        "booking_quantity": "INT64",
        "booking_token": "STRING",
        "user_id": "STRING",
        "booking_amount": "NUMERIC",
        "booking_is_cancelled": "BOOLEAN",
        "booking_is_used": "BOOLEAN",
        "booking_used_date": "DATETIME",
        "booking_cancellation_date": "DATETIME",
        "booking_cancellation_reason": "STRING",
    },
    "favorite": {
        "id": "STRING",
        "userId": "STRING",
        "offerId": "STRING",
        "mediationId": "STRING",
        "lastupdate": "DATETIME",
        "dateCreated": "DATETIME",
    },
    "offer": {
        "offer_id_at_providers": "STRING",
        "offer_modified_at_last_provider_date": "DATETIME",
        "offer_id": "STRING",
        "offer_creation_date": "DATETIME",
        "product_id": "STRING",
        "venue_id": "STRING",
        "offer_last_provider_id": "STRING",
        "booking_email": "STRING",
        "offer_is_active": "BOOLEAN",
        "offer_type": "STRING",
        "offer_name": "STRING",
        "offer_description": "STRING",
        "offer_conditions": "STRING",
        "offer_age_min": "INT64",
        "offer_age_max": "INT64",
        "offer_url": "STRING",
        "offer_media_urls": "STRING",
        "offer_duration_minutes": "INT64",
        "offer_is_national": "BOOLEAN",
        "offer_extra_data": "STRING",
        "offer_is_duo": "BOOLEAN",
        "offer_fields_updated": "STRING",
        "offer_withdrawal_details": "STRING",
    },
    "offerer": {
        "offerer_is_active": "BOOLEAN",
        "offerer_thumb_count": "INT64",
        "offerer_first_thumb_dominant_color": "BYTES",
        "offerer_id_at_providers": "STRING",
        "offerer_date_modified_at_last_provider": "DATETIME",
        "offerer_address": "STRING",
        "offerer_postal_code": "STRING",
        "offerer_city": "STRING",
        "offerer_validation_token": "STRING",
        "offerer_id": "STRING",
        "offerer_creation_date": "DATETIME",
        "offerer_name": "STRING",
        "offerer_siren": "STRING",
        "offerer_last_provider_id": "STRING",
        "offerer_fields_updated": "STRING",
    },
    "payment": {
        "id": "STRING",
        "author": "STRING",
        "comment": "STRING",
        "recipientName": "STRING",
        "iban": "STRING",
        "bic": "STRING",
        "bookingId": "STRING",
        "amount": "NUMERIC",
        "reimbursementRule": "STRING",
        "transactionEndToEndId": "STRING",
        "recipientSiren": "STRING",
        "reimbursementRate": "NUMERIC",
        "transactionLabel": "STRING",
        "paymentMessageId": "INT64",
    },
    "payment_status": {
        "id": "STRING",
        "paymentId": "STRING",
        "date": "DATETIME",
        "status": "STRING",
        "detail": "STRING",
    },
    "product": {
        "thumbCount": "INT64",
        "idAtProviders": "STRING",
        "dateModifiedAtLastProvider": "DATETIME",
        "id": "STRING",
        "type": "STRING",
        "name": "STRING",
        "description": "STRING",
        "conditions": "STRING",
        "ageMin": "INT64",
        "ageMax": "INT64",
        "mediaUrls": "STRING",
        "durationMinutes": "INT64",
        "lastProviderId": "STRING",
        "isNational": "BOOLEAN",
        "extraData": "STRING",
        "owningOffererId": "STRING",
        "url": "STRING",
        "fieldsUpdated": "STRING",
        "isGcuCompatible": "BOOLEAN",
    },
    "stock": {
        "stock_id_at_providers": "STRING",
        "stock_modified_at_last_provider_date": "DATETIME",
        "stock_id": "STRING",
        "stock_modified_date": "DATETIME",
        "stock_price": "NUMERIC",
        "stock_quantity": "INT64",
        "stock_booking_limit_date": "DATETIME",
        "stock_last_provider_id": "STRING",
        "offer_id": "STRING",
        "stock_is_soft_deleted": "BOOLEAN",
        "stock_beginning_date": "DATETIME",
        "stock_creation_date": "DATETIME",
        "stock_fields_updated": "STRING",
        "stock_has_been_migrated": "BOOLEAN",
    },
    "user": {
        "user_id": "STRING",
        "user_validation_token": "STRING",
        "user_email": "STRING",
        "user_password": "BYTES",
        "user_public_name": "STRING",
        "user_creation_date": "DATETIME",
        "user_department_code": "STRING",
        "user_is_beneficiary": "BOOLEAN",
        "user_is_admin": "BOOLEAN",
        "user_reset_password_token": "STRING",
        "user_reset_password_token_validity_limit": "DATETIME",
        "user_first_name": "STRING",
        "user_last_name": "STRING",
        "user_postal_code": "STRING",
        "user_phone_number": "STRING",
        "user_birth_date": "DATETIME",
        "user_needs_to_fill_cultural_survey": "BOOLEAN",
        "user_cultural_survey_id": "STRING",
        "user_civility": "STRING",
        "user_activity": "STRING",
        "user_cultural_survey_filled_date": "DATETIME",
        "user_has_seen_tutorials": "BOOLEAN",
        "user_address": "STRING",
        "user_city": "STRING",
        "user_lastConnectionDate": "DATETIME",
        "user_is_active": "BOOLEAN",
        "user_suspension_reason": "STRING",
    },
    "venue": {
        "venue_thumb_count": "INT64",
        "venue_first_thumb_dominant_color": "BYTES",
        "venue_id_at_providers": "STRING",
        "venue_modified_at_last_provider_date": "DATETIME",
        "venue_address": "STRING",
        "venue_postal_code": "STRING",
        "venue_city": "STRING",
        "venue_id": "STRING",
        "venue_name": "STRING",
        "venue_siret": "STRING",
        "venue_department_code": "STRING",
        "venue_latitude": "NUMERIC",
        "venue_longitude": "NUMERIC",
        "venue_managing_offerer_id": "STRING",
        "venue_booking_email": "STRING",
        "venue_last_provider_id": "STRING",
        "venue_is_virtual": "BOOLEAN",
        "venue_comment": "STRING",
        "venue_validation_token": "STRING",
        "venue_public_name": "STRING",
        "venue_fields_updated": "STRING",
        "venue_type_id": "STRING",
        "venue_label_id": "STRING",
        "venue_creation_date": "DATETIME",
    },
    "venue_label": {
        "id": "STRING",
        "label": "STRING",
        "lastupdate": "DATETIME",
    },
    "venue_type": {
        "id": "STRING",
        "label": "STRING",
        "lastupdate": "DATETIME",
    },
    "region_department": {
        "num_dep": "STRING",
        "dep_name": "STRING",
        "region_name": "STRING",
    },
    "deposit": {
        "id": "STRING",
        "userId": "STRING",
        "amount": "NUMERIC",
        "expirationDate": "DATETIME",
    },
}
