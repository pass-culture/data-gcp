import uuid

TEST_DATASET = f"test_{uuid.uuid1().hex}"
GCP_REGION = "europe-west1"
GCP_PROJECT = "passculture-data-ehp"
BIGQUERY_SCHEMAS = {
    "applicative_database_booking": {
        "booking_is_active": "BOOLEAN",
        "booking_id": "STRING",
        "individual_booking_id": "STRING",
        "booking_creation_date": "DATETIME",
        "recommendation_id": "STRING",
        "stock_id": "STRING",
        "booking_quantity": "INT64",
        "booking_token": "STRING",
        "user_id": "STRING",
        "booking_amount": "NUMERIC",
        "booking_status": "STRING",
        "booking_is_cancelled": "BOOLEAN",
        "booking_is_used": "BOOLEAN",
        "booking_used_date": "DATETIME",
        "booking_cancellation_date": "DATETIME",
        "booking_cancellation_reason": "STRING",
        "booking_reimbursement_date": "DATETIME",
    },
    "applicative_database_individual_booking": {
        "individual_booking_id": "STRING",
        "user_id": "STRING",
        "deposit_id": "STRING",
    },
    "applicative_database_educational_institution": {
        "educational_institution_id": "STRING",
        "educational_institution_institution_id": "STRING",
    },
    "applicative_database_user_suspension": {
        "id": "STRING",
        "userId": "STRING",
        "eventType": "STRING",
        "eventDate": "DATETIME",
        "actorUserId": "STRING",
        "reasonCode": "STRING",
    },
    "applicative_database_favorite": {
        "id": "STRING",
        "userId": "STRING",
        "offerId": "STRING",
        "mediationId": "STRING",
        "lastupdate": "DATETIME",
        "dateCreated": "DATETIME",
    },
    "applicative_database_offer": {
        "offer_id_at_providers": "STRING",
        "offer_modified_at_last_provider_date": "DATETIME",
        "offer_id": "STRING",
        "offer_creation_date": "DATETIME",
        "offer_product_id": "STRING",
        "venue_id": "STRING",
        "offer_last_provider_id": "STRING",
        "booking_email": "STRING",
        "offer_is_active": "BOOLEAN",
        "offer_subcategoryId": "STRING",
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
        "offer_validation": "STRING",
        "offer_withdrawal_details": "STRING",
        "offer_withdrawalDelay": "INT64",
        "offer_withdrawal_type": "STRING",
    },
    "offer_extracted_data": {
        "offer_id": "STRING",
        "author": "STRING",
        "performer": "STRING",
        "musicType": "STRING",
        "musicSubtype": "STRING",
        "stageDirector": "STRING",
        "theater_movie_id": "STRING",
        "theater_room_id": "STRING",
        "showType": "STRING",
        "showSubType": "STRING",
        "speaker": "STRING",
        "rayon": "STRING",
        "movie_type": "STRING",
        "visa": "STRING",
        "releaseDate": "STRING",
        "genres": "STRING",
        "companies": "STRING",
        "countries": "STRING",
        "casting": "STRING",
        "isbn": "STRING",
    },
    "offer_tags": {
        "offer_id": "STRING",
        "tag": "STRING",
    },
    "applicative_database_offerer": {
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
        "offerer_validation_date": "DATETIME",
        "offerer_name": "STRING",
        "offerer_siren": "STRING",
        "offerer_last_provider_id": "STRING",
        "offerer_fields_updated": "STRING",
    },
    "applicative_database_payment": {
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
    "applicative_database_payment_status": {
        "id": "STRING",
        "paymentId": "STRING",
        "date": "DATETIME",
        "status": "STRING",
        "detail": "STRING",
    },
    "applicative_database_product": {
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
    "applicative_database_stock": {
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
        "number_of_tickets": "INT64",
        "educational_price_detail": "STRING",
    },
    "stock_booking_information": {
        "stock_id": "STRING",
        "booking_quantity": "INTEGER",
        "bookings_cancelled": "INTEGER",
        "bookings_paid": "INTEGER",
    },
    "available_stock_information": {
        "stock_id": "STRING",
        "available_stock_information": "INTEGER",
    },
    "enriched_stock_data": {
        "stock_id": "STRING",
        "offer_id": "STRING",
        "offer_name": "STRING",
        "offerer_id": "STRING",
        "offer_subcategoryId": "STRING",
        "venue_department_code": "STRING",
        "stock_creation_date": "DATETIME",
        "stock_booking_limit_date": "DATETIME",
        "stock_beginning_date": "DATETIME",
        "available_stock_information": "INTEGER",
        "stock_quantity": "INTEGER",
        "booking_quantity": "INTEGER",
        "booking_cancelled": "INTEGER",
        "booking_paid": "INTEGER",
        "stock_price": "NUMERIC",
    },
    "enriched_deposit_data": {
        "deposit_id": "STRING",
        "user_id": "STRING",
        "deposit_amount": "INTEGER",
        "deposit_theoretical_amount_spent": "INTEGER",
        "deposit_actual_amount_spent": "INTEGER",
        "deposit_theoretical_amount_spent_in_digital_goods": "INTEGER",
        "deposit_rank_desc": "INTEGER",
    },
    "enriched_offer_data": {
        "offerer_id": "STRING",
        "offerer_name": "STRING",
        "venue_id": "STRING",
        "venue_name": "STRING",
        "venue_department_code": "STRING",
        "offer_id": "STRING",
        "offer_product_id": "STRING",
        "item_id": "STRING",
        "offer_name": "STRING",
        "URL": "STRING",
        "is_national": "STRING",
        "is_active": "STRING",
        "offer_validation": "STRING",
        "offer_subcategoryId": "STRING",
        "last_stock_price": "NUMERIC",
        "offer_creation_date": "DATETIME",
        "offer_is_duo": "BOOLEAN",
        "offer_is_underage_selectable": "BOOLEAN",
        "offer_is_bookable": "BOOLEAN",
        "venue_is_virtual": "BOOLEAN",
        "physical_goods": "BOOLEAN",
        "outing": "BOOLEAN",
        "booking_cnt": "FLOAT",
        "booking_cancelled_cnt": "FLOAT",
        "booking_confirm_cnt": "FLOAT",
        "favourite_cnt": "FLOAT",
        "stock": "FLOAT",
        "offer_humanized_id": "STRING",
        "passculture_pro_url": "STRING",
        "webapp_url": "STRING",
        "first_booking_cnt": "INTEGER",
        "offer_tag": "STRING",
        "author": "STRING",
        "performer": "STRING",
        "stageDirector": "STRING",
        "theater_movie_id": "STRING",
        "theater_room_id": "STRING",
        "speaker": "STRING",
        "rayon": "STRING",
        "movie_type": "STRING",
        "visa": "STRING",
        "releaseDate": "STRING",
        "genres": "STRING",
        "companies": "STRING",
        "countries": "STRING",
        "casting": "STRING",
        "type": "STRING",
        "subType": "STRING",
        "isbn": "STRING",
        "book_editor": "STRING",
    },
    "applicative_database_user": {
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
        "user_needs_to_fill_cultural_survey": "BOOLEAN",
        "user_cultural_survey_id": "STRING",
        "user_civility": "STRING",
        "user_activity": "STRING",
        "user_cultural_survey_filled_date": "DATETIME",
        "user_address": "STRING",
        "user_city": "STRING",
        "user_lastConnectionDate": "DATETIME",
        "user_is_active": "BOOLEAN",
        "user_suspension_reason": "STRING",
        "user_age": "INT64",
        "user_has_completed_idCheck": "BOOLEAN",
        "user_phone_validation_status": "BOOLEAN",
        "user_has_validated_email": "BOOLEAN",
        "user_has_enabled_marketing_push": "BOOLEAN",
        "user_has_enabled_marketing_email": "BOOLEAN",
        "user_birth_date": "DATETIME",
        "user_role": "STRING",
        "user_school_type": "STRING",
    },
    "applicative_database_venue": {
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
        "venue_is_permanent": "BOOLEAN",
        "venue_comment": "STRING",
        "venue_validation_token": "STRING",
        "venue_public_name": "STRING",
        "venue_fields_updated": "STRING",
        "venue_type_id": "STRING",
        "venue_label_id": "STRING",
        "venue_creation_date": "DATETIME",
        "venue_type_code": "STRING",
        "business_unit_id": "STRING",
    },
    "applicative_database_venue_label": {
        "id": "STRING",
        "label": "STRING",
        "lastupdate": "DATETIME",
    },
    "applicative_database_venue_type": {
        "id": "STRING",
        "label": "STRING",
        "lastupdate": "DATETIME",
    },
    "region_department": {
        "num_dep": "STRING",
        "dep_name": "STRING",
        "region_name": "STRING",
    },
    "applicative_database_deposit": {
        "id": "STRING",
        "userId": "STRING",
        "amount": "NUMERIC",
        "source": "STRING",
        "expirationDate": "DATETIME",
        "dateCreated": "DATETIME",
        "type": "STRING",
    },
    "applicative_database_offer_report": {
        "offer_report_id": "STRING",
        "offer_report_user_id": "STRING",
        "offer_report_offer_id": "STRING",
        "offer_report_reason": "STRING",
        "offer_report_custom_reason_content": "STRING",
        "offer_report_date": "DATETIME",
    },
    "subcategories": {
        "id": "STRING",
        "category_id": "STRING",
        "is_event": "BOOLEAN",
        "is_physical_deposit": "BOOLEAN",
        "is_digital_deposit": "BOOLEAN",
    },
    "eple": {
        "id_etablissement": "STRING",
        "nom_etablissement": "STRING",
        "libelle_academie": "STRING",
        "code_departement": "STRING",
    },
    "academie_dept": {
        "code_dpt": "STRING",
        "libelle_academie": "STRING",
        "libelle_region": "STRING",
    },
    "applicative_database_collective_booking": {
        "collective_booking_id": "STRING",
        "booking_id": "STRING",
        "collective_booking_creation_date": "DATETIME",
        "collective_booking_used_date": "DATETIME",
        "collective_stock_id": "STRING",
        "venue_id": "STRING",
        "offerer_id": "STRING",
        "collective_booking_cancellation_date": "DATETIME",
        "collective_booking_cancellation_limit_date": "DATETIME",
        "collective_booking_cancellation_reason": "STRING",
        "collective_booking_status": "STRING",
        "collective_booking_reimbursement_date": "DATETIME",
        "educational_institution_id": "STRING",
        "educational_year_id": "STRING",
        "collective_booking_confirmation_date": "DATETIME",
        "collective_booking_confirmation_limit_date": "DATETIME",
        "educational_redactor_id": "STRING",
    },
    "applicative_database_collective_offer": {
        "collective_offer_audio_disability_compliant": "BOOLEAN",
        "collective_offer_mental_disability_compliant": "BOOLEAN",
        "collective_offer_motor_disability_compliant": "BOOLEAN",
        "collective_offer_visual_disability_compliant": "BOOLEAN",
        "collective_offer_last_validation_date": "DATETIME",
        "collective_offer_validation": "STRING",
        "collective_offer_id": "STRING",
        "offer_id": "STRING",
        "collective_offer_is_active": "BOOLEAN",
        "venue_id": "STRING",
        "institution_id": "STRING",
        "collective_offer_name": "STRING",
        "collective_offer_booking_email": "STRING",
        "collective_offer_description": "STRING",
        "collective_offer_duration_minutes": "INT64",
        "collective_offer_creation_date": "DATETIME",
        "collective_offer_subcategory_id": "STRING",
        "collective_offer_date_updated": "DATETIME",
        "collective_offer_students": "STRING",
        "collective_offer_contact_email": "STRING",
        "collective_offer_contact_phone": "STRING",
        "collective_offer_offer_venue": "STRING",
    },
    "applicative_database_collective_offer_template": {
        "collective_offer_audio_disability_compliant": "BOOLEAN",
        "collective_offer_mental_disability_compliant": "BOOLEAN",
        "collective_offer_motor_disability_compliant": "BOOLEAN",
        "collective_offer_visual_disability_compliant": "BOOLEAN",
        "collective_offer_visual_disability_compliant": "BOOLEAN",
        "collective_offer_validation": "STRING",
        "collective_offer_id": "STRING",
        "offer_id": "STRING",
        "collective_offer_is_active": "BOOLEAN",
        "venue_id": "STRING",
        "collective_offer_name": "STRING",
        "collective_offer_description": "STRING",
        "collective_offer_duration_minutes": "INT64",
        "collective_offer_creation_date": "DATETIME",
        "collective_offer_subcategory_id": "STRING",
        "collective_offer_date_updated": "DATETIME",
        "collective_offer_students": "STRING",
        "collecive_offer_price_detail": "STRING",
        "collective_booking_email": "STRING",
        "collective_offer_contact_email": "STRING",
        "collective_offer_contact_phone": "STRING",
        "collective_offer_offer_venue": "STRING",
        "collective_offer_las_validation_type": "STRING",
    },
    "applicative_database_collective_stock": {
        "collective_stock_id": "STRING",
        "stock_id": "STRING",
        "collective_stock_creation_date": "DATETIME",
        "collective_stock_modification_date": "DATETIME",
        "collective_stock_beginning_date_time": "DATETIME",
        "collective_offer_id": "STRING",
        "collective_stock_price": "NUMERIC",
        "collective_stock_booking_limit_date_time": "DATETIME",
        "collective_stock_number_of_tickets": "INT64",
        "collective_stock_price_detail": "STRING",
    },
    "applicative_database_venue_contact": {
        "venue_contact_id": "STRING",
        "venue_contact_venue_id": "STRING",
        "venue_contact_email": "STRING",
        "venue_contact_website": "STRING",
        "venue_contact_phone_number": "STRING",
        "venue_contact_social_medias": "STRING",
    },
    "applicative_database_mediation": {
        "thumbCount": "INTEGER",
        "idAtProviders": "STRING",
        "dateModifiedAtLastProvider": "DATETIME",
        "id": "STRING",
        "dateCreated": "DATETIME",
        "authorId": "STRING",
        "lastProviderId": "STRING",
        "offerId": "STRING",
        "credit": "STRING",
        "isActive": "BOOLEAN",
        "fieldsUpdated": "STRING",
    },
    "isbn_editor": {
        "isbn": "STRING",
        "book_editor": "STRING",
    },
    "offer_item_ids": {
        "offer_id": "STRING",
        "item_id": "STRING",
    },
}
