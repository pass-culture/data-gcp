from datetime import datetime
from decimal import Decimal

TEST_TABLE_PREFIX = ""

# Enriched_offer_data
ENRICHED_OFFER_DATA_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
            "booking_quantity": "2",
            "booking_creation_date": "2019-11-20",
            "booking_token": "ABC123",
            "booking_amount": "0",
            "booking_is_cancelled": False,
            "booking_is_used": False,
            "booking_used_date": "2019-11-22",
        },
    ],
    "favorite": [
        {"id": "1", "offerId": "3", "userId": "1"},
        {"id": "2", "offerId": "4", "userId": "2"},
        {"id": "3", "offerId": "3", "userId": "3"},
    ],
    "offer": [
        {
            "venue_id": "1",
            "product_id": "1",
            "offer_id": "3",
            "offer_subcategoryId": "SEANCE_CINE",
            "offer_name": "Test",
            "offer_is_active": True,
            "offer_media_urls": '["https://url.test", "https://someurl.test"]',
            "offer_url": None,
            "offer_is_national": False,
            "offer_creation_date": "2019-11-20",
            "offer_is_duo": False,
            "offer_fields_updated": "{}",
            "offer_validation": "APPROVED",
            "offer_is_educational": False,
        },
        {
            "venue_id": "2",
            "product_id": "2",
            "offer_id": "4",
            "offer_subcategoryId": "LIVRE_PAPIER",
            "offer_name": "RIP Dylan Rieder",
            "offer_is_active": True,
            "offer_media_urls": '["https://url.test", "https://someurl.test"]',
            "offer_url": None,
            "offer_is_national": False,
            "offer_creation_date": "2019-11-20",
            "offer_is_duo": False,
            "offer_fields_updated": "{}",
            "offer_validation": "APPROVED",
            "offer_is_educational": False,
        },
    ],
    "offer_extracted_data": [
        {
            "offer_id": "3",
            "author": "Tarantino",
            "performer": "Uma Turman",
            "musicType": None,
            "musicSubtype": None,
            "stageDirector": None,
            "theater_movie_id": "255795",
            "theater_room_id": "p0709",
            "showType": "Action",
            "showSubType": None,
            "speaker": None,
            "rayon": None,
            "movie_type": "feature_film",
            "visa": "154181",
            "releaseDate": "2021-07-07",
            "genres": '["adventure","animation","comedy","family"]',
            "companies": '[{"activity":"production","company":{"name":"universal pictures"}}]',
            "countries": '["usa"]',
            "casting": "[]",
            "isbn": None,
        },
        {
            "offer_id": "4",
            "author": "Kevin Francois",
            "performer": None,
            "musicType": None,
            "musicSubtype": None,
            "stageDirector": None,
            "theater_movie_id": None,
            "theater_room_id": None,
            "showType": "Polar",
            "showSubType": None,
            "speaker": None,
            "rayon": None,
            "movie_type": None,
            "visa": None,
            "releaseDate": None,
            "genres": None,
            "companies": None,
            "countries": None,
            "casting": None,
            "isbn": None,
        },
    ],
    "offer_tags": [
        {
            "offer_id": "3",
            "tag": "none",
        },
        {
            "offer_id": "4",
            "tag": "none",
        },
    ],
    "offerer": [
        {
            "offerer_id": "3",
            "offerer_thumb_count": "0",
            "offerer_is_active": True,
            "offerer_postal_code": "93100",
            "offerer_city": "Montreuil",
            "offerer_creation_date": "2019-11-20",
            "offerer_name": "Test Offerer",
            "offerer_siren": "123456789",
            "offerer_fields_updated": "{}",
        },
        {
            "offerer_id": "4",
            "offerer_siren": "234567890",
            "offerer_thumb_count": "0",
            "offerer_is_active": True,
            "offerer_postal_code": "93100",
            "offerer_city": "Montreuil",
            "offerer_creation_date": "2019-11-20",
            "offerer_name": "Test Offerer",
            "offerer_fields_updated": "{}",
        },
    ],
    "payment": [
        {
            "bookingId": "4",
            "id": "1",
            "amount": "10",
            "reimbursementRule": "test",
            "reimbursementRate": "1",
            "recipientName": "Toto",
            "recipientSiren": "123456789",
            "author": "test",
        },
    ],
    "payment_status": [
        {"paymentId": "1", "id": "1", "date": "2019-01-01", "status": "PENDING"},
    ],
    "product": [
        {
            "id": "1",
            "type": "EventType.CINEMA",
            "thumbCount": "0",
            "name": "Livre",
            "mediaUrls": '["https://url.test", "https://someurl.test"]',
            "fieldsUpdated": "{}",
            "url": None,
            "isNational": False,
        },
        {
            "id": "2",
            "type": "ThingType.LIVRE_EDITION",
            "thumbCount": "0",
            "name": "Livre",
            "mediaUrls": '["https://url.test", "https://someurl.test"]',
            "fieldsUpdated": "{}",
            "url": None,
            "isNational": False,
        },
    ],
    "stock": [
        {
            "stock_id": "1",
            "offer_id": "3",
            "stock_creation_date": "2019-11-01",
            "stock_quantity": "10",
            "stock_booking_limit_date": "2019-11-23",
            "stock_beginning_date": "2019-11-24",
            "stock_is_soft_deleted": False,
            "stock_modified_date": "2019-11-20",
            "stock_price": "0",
            "stock_fields_updated": "{}",
        },
        {
            "stock_id": "2",
            "offer_id": "4",
            "stock_creation_date": "2019-10-01",
            "stock_quantity": "12",
            "stock_is_soft_deleted": False,
            "stock_modified_date": "2019-11-20",
            "stock_price": "0",
            "stock_booking_limit_date": None,
            "stock_beginning_date": None,
            "stock_fields_updated": "{}",
        },
    ],
    "enriched_stock_data": [
        {
            "stock_id": "1",
            "offer_id": "3",
            "available_stock_information": "8",
            "stock_booking_limit_date": "2019-11-23",
            "stock_beginning_date": "2019-11-24",
            "stock_price": "0",
        },
        {
            "stock_id": "2",
            "offer_id": "4",
            "available_stock_information": "12",
            "stock_booking_limit_date": None,
            "stock_beginning_date": None,
            "stock_price": "0",
        },
    ],
    "user": [
        {
            "user_id": "1",
            "user_email": "test@email.com",
            "user_is_beneficiary": True,
            "user_is_admin": False,
            "user_postal_code": "93100",
            "user_department_code": "93",
            "user_public_name": "Test",
            "user_creation_date": "2018-11-20",
            "user_needs_to_fill_cultural_survey": True,
            "user_cultural_survey_filled_date": None,
        },
        {
            "user_id": "2",
            "user_email": "other@test.com",
            "user_is_beneficiary": True,
            "user_is_admin": False,
            "user_postal_code": "93100",
            "user_department_code": "93",
            "user_public_name": "Test",
            "user_creation_date": "2018-11-20",
            "user_needs_to_fill_cultural_survey": True,
            "user_cultural_survey_filled_date": None,
        },
        {
            "user_id": "3",
            "user_email": "louie.lopez@test.com",
            "user_is_beneficiary": True,
            "user_is_admin": False,
            "user_postal_code": "93100",
            "user_department_code": "93",
            "user_public_name": "Test",
            "user_creation_date": "2018-11-20",
            "user_needs_to_fill_cultural_survey": True,
            "user_cultural_survey_filled_date": None,
        },
    ],
    "venue": [
        {
            "venue_managing_offerer_id": "3",
            "venue_id": "1",
            "venue_siret": "12345678900026",
            "venue_thumb_count": "0",
            "venue_name": "Test Venue",
            "venue_postal_code": "93",
            "venue_city": "Montreuil",
            "venue_department_code": "93",
            "venue_is_virtual": False,
            "venue_fields_updated": "{}",
        },
        {
            "venue_managing_offerer_id": "4",
            "venue_id": "2",
            "venue_siret": "23456789000067",
            "venue_thumb_count": "0",
            "venue_name": "Test Venue",
            "venue_postal_code": "93",
            "venue_city": "Montreuil",
            "venue_department_code": "93",
            "venue_is_virtual": False,
            "venue_fields_updated": "{}",
        },
    ],
    "subcategories": [
        {
            "id": "SEANCE_CINE",
            "category_id": "CINEMA",
            "is_physical_deposit": False,
            "is_event": True,
        },
        {
            "id": "LIVRE_PAPIER",
            "category_id": "LIVRE",
            "is_physical_deposit": True,
            "is_event": False,
        },
    ],
}
ENRICHED_OFFER_DATA_EXPECTED = [
    {
        "offer_id": "3",
        "offerer_id": "3",
        "offerer_name": "Test Offerer",
        "venue_id": "1",
        "venue_name": "Test Venue",
        "venue_department_code": "93",
        "offer_name": "Test",
        "offer_subcategoryId": "SEANCE_CINE",
        "last_stock_price": 0.0,
        "offer_creation_date": datetime(2019, 11, 20, 0, 0),
        "offer_is_duo": False,
        "venue_is_virtual": False,
        "physical_goods": False,
        "outing": True,
        "booking_cnt": 1.0,
        "booking_cancelled_cnt": 0.0,
        "booking_confirm_cnt": 0.0,
        "favourite_cnt": 2.0,
        "stock": 10.0,
        "offer_humanized_id": "AM",
        "passculture_pro_url": "https://passculture.pro/offres/AM/edition",
        "webapp_url": "https://passculture.app/offre/3",
        "first_booking_cnt": 1,
        "offer_tag": "none",
        "author": "Tarantino",
        "performer": "Uma Turman",
        "stageDirector": None,
        "type": "Action",
        "subType": None,
        "theater_movie_id": "255795",
        "theater_room_id": "p0709",
        "speaker": None,
        "rayon": None,
        "movie_type": "feature_film",
        "visa": "154181",
        "releaseDate": "2021-07-07",
        "genres": '["adventure","animation","comedy","family"]',
        "companies": '[{"activity":"production","company":{"name":"universal pictures"}}]',
        "countries": '["usa"]',
        "casting": "[]",
        "isbn": None,
        "offer_is_educational": False,
        "offer_is_underage_selectable": True,
        "offer_is_bookable": False,
    },
    {
        "offer_id": "4",
        "offerer_id": "4",
        "offerer_name": "Test Offerer",
        "venue_id": "2",
        "venue_name": "Test Venue",
        "venue_department_code": "93",
        "offer_name": "RIP Dylan Rieder",
        "offer_subcategoryId": "LIVRE_PAPIER",
        "last_stock_price": 0.0,
        "offer_creation_date": datetime(2019, 11, 20, 0, 0),
        "offer_is_duo": False,
        "venue_is_virtual": False,
        "physical_goods": True,
        "outing": False,
        "booking_cnt": 0.0,
        "booking_cancelled_cnt": 0.0,
        "booking_confirm_cnt": 0.0,
        "favourite_cnt": 1.0,
        "stock": 12.0,
        "offer_humanized_id": "AQ",
        "passculture_pro_url": "https://passculture.pro/offres/AQ/edition",
        "webapp_url": "https://passculture.app/offre/4",
        "first_booking_cnt": None,
        "offer_tag": "none",
        "author": "Kevin Francois",
        "performer": None,
        "stageDirector": None,
        "speaker": None,
        "rayon": None,
        "type": "Polar",
        "subType": None,
        "theater_movie_id": None,
        "theater_room_id": None,
        "movie_type": None,
        "visa": None,
        "releaseDate": None,
        "genres": None,
        "companies": None,
        "countries": None,
        "casting": None,
        "isbn": None,
        "offer_is_educational": False,
        "offer_is_underage_selectable": True,
        "offer_is_bookable": True,
    },
]

# Enriched_stock_data
ENRICHED_STOCK_DATA_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
            "booking_quantity": "2",
            "booking_creation_date": "2019-11-20",
            "booking_token": "ABC123",
            "booking_amount": "0",
            "booking_is_cancelled": False,
            "booking_is_used": False,
            "booking_used_date": "2019-11-22",
        }
    ],
    "offer": [
        {
            "venue_id": "1",
            "product_id": "1",
            "offer_id": "3",
            "offer_subcategoryId": "SEANCE_CINE",
            "offer_name": "Test",
            "offer_is_active": True,
            "offer_media_urls": '["https://url.test", "https://someurl.test"]',
            "offer_url": None,
            "offer_is_national": False,
            "offer_creation_date": "2019-11-20",
            "offer_is_duo": False,
            "offer_fields_updated": "{}",
        },
        {
            "venue_id": "1",
            "product_id": "2",
            "offer_id": "2",
            "offer_subcategoryId": "LIVRE_PAPIER",
            "offer_name": "Test bis",
            "offer_is_active": True,
            "offer_media_urls": '["https://url.test", "https://someurl.test"]',
            "offer_url": None,
            "offer_is_national": False,
            "offer_creation_date": "2019-11-20",
            "offer_is_duo": False,
            "offer_fields_updated": "{}",
        },
    ],
    "offerer": [
        {
            "offerer_id": "3",
            "offerer_thumb_count": "0",
            "offerer_is_active": True,
            "offerer_postal_code": "93100",
            "offerer_city": "Montreuil",
            "offerer_creation_date": "2019-11-20",
            "offerer_name": "Test Offerer",
            "offerer_siren": "123456789",
            "offerer_fields_updated": "{}",
        }
    ],
    "payment": [
        {
            "bookingId": "4",
            "id": "1",
            "amount": "10",
            "reimbursementRule": "test",
            "reimbursementRate": "1",
            "recipientName": "Toto",
            "recipientSiren": "123456789",
            "author": "test",
        }
    ],
    "payment_status": [
        {"paymentId": "1", "id": "1", "date": "2019-01-01", "status": "PENDING"}
    ],
    "product": [
        {
            "id": "1",
            "type": "EventType.CINEMA",
            "thumbCount": "0",
            "name": "Livre",
            "mediaUrls": '["https://url.test", "https://someurl.test"]',
            "fieldsUpdated": "{}",
            "url": None,
            "isNational": False,
        },
        {
            "id": "1",
            "type": "ThingType.LIVRE_EDITION",
            "thumbCount": "0",
            "name": "Livre",
            "mediaUrls": '["https://url.test", "https://someurl.test"]',
            "fieldsUpdated": "{}",
            "url": None,
            "isNational": False,
        },
    ],
    "stock": [
        {
            "stock_id": "1",
            "offer_id": "3",
            "stock_creation_date": "2019-11-01",
            "stock_quantity": "10",
            "stock_booking_limit_date": "2019-11-23",
            "stock_beginning_date": "2019-11-24",
            "stock_is_soft_deleted": False,
            "stock_modified_date": "2019-11-20",
            "stock_price": "0",
            "stock_fields_updated": "{}",
        },
        {
            "stock_id": "2",
            "offer_id": "2",
            "stock_creation_date": "2019-10-01",
            "stock_quantity": "12",
            "stock_booking_limit_date": None,
            "stock_beginning_date": None,
            "stock_is_soft_deleted": False,
            "stock_modified_date": "2019-11-20",
            "stock_price": "0",
            "stock_fields_updated": "{}",
        },
    ],
    "user": [
        {
            "user_id": "1",
            "user_email": "test@email.com",
            "user_is_beneficiary": True,
            "user_is_admin": False,
            "user_postal_code": "93100",
            "user_department_code": "93",
            "user_public_name": "Test",
            "user_creation_date": "2018-11-20",
            "user_needs_to_fill_cultural_survey": True,
            "user_cultural_survey_filled_date": None,
        },
        {
            "user_id": "2",
            "user_email": "other@test.com",
            "user_is_beneficiary": True,
            "user_is_admin": False,
            "user_postal_code": "93100",
            "user_department_code": "93",
            "user_public_name": "Test",
            "user_creation_date": "2018-11-20",
            "user_needs_to_fill_cultural_survey": True,
            "user_cultural_survey_filled_date": None,
        },
    ],
    "venue": [
        {
            "venue_managing_offerer_id": "3",
            "venue_id": "1",
            "venue_siret": None,
            "venue_thumb_count": "0",
            "venue_name": "Test Venue",
            "venue_postal_code": None,
            "venue_city": None,
            "venue_department_code": None,
            "venue_is_virtual": True,
            "venue_fields_updated": "{}",
        }
    ],
}

ENRICHED_STOCK_DATA_EXPECTED = [
    {
        "stock_id": "1",
        "offer_id": "3",
        "offer_name": "Test",
        "offerer_id": "3",
        "offer_subcategoryId": "SEANCE_CINE",
        "venue_department_code": None,
        "stock_creation_date": datetime(2019, 11, 1),
        "stock_booking_limit_date": datetime(2019, 11, 23),
        "stock_beginning_date": datetime(2019, 11, 24),
        "available_stock_information": 8,
        "stock_quantity": 10,
        "booking_quantity": 2,
        "booking_cancelled": 0,
        "booking_paid": 2,
        "stock_price": 0,
    },
    {
        "stock_id": "2",
        "offer_id": "2",
        "offer_name": "Test bis",
        "offerer_id": "3",
        "offer_subcategoryId": "LIVRE_PAPIER",
        "venue_department_code": None,
        "stock_creation_date": datetime(2019, 10, 1),
        "stock_booking_limit_date": None,
        "stock_beginning_date": None,
        "available_stock_information": 12,
        "stock_quantity": 12,
        "booking_quantity": 0,
        "booking_cancelled": 0,
        "booking_paid": 0,
        "stock_price": 0,
    },
]

# Enriched_user_data => user 1 is beneficiary and its department has to exist in region_department, has one used booking
# on digital goods. This booking is corresponding to an offer, with an offered, stock and venue
ENRICHED_USER_DATA_INPUT = {
    "user": [
        {
            "user_id": "1",
            "user_is_beneficiary": True,
            "user_department_code": 93,
            "user_postal_code": "93000",
            "user_activity": "Inactif",
            "user_civility": "Mme",
            "user_creation_date": datetime.now().replace(microsecond=0),
            "user_has_seen_tutorials": True,
            "user_cultural_survey_filled_date": datetime.now().replace(microsecond=0),
            "user_is_active": True,
            "user_age": 18,
            "user_has_completed_idCheck": True,
            "user_phone_validation_status": True,
            "user_has_validated_email": True,
            "user_has_enabled_marketing_push": True,
            "user_has_enabled_marketing_email": True,
            "user_birth_date": datetime.now().replace(microsecond=0),
            "user_role": "BENEFICIARY",
            "user_school_type": "Lycée agricole",
            "user_subscription_state": "account_created",
        }
    ],
    "user_suspension": [
        {
            "id": "1",
            "userId": "1",
            "eventType": "UNSUSPENDED",
            "eventDate": datetime.now().replace(microsecond=0),
            "actorUserId": "1388409",
            "reasonCode": None,
        }
    ],
    "deposit": [
        {
            "id": "1",
            "userId": "1",
            "amount": 500,
            "expirationDate": datetime.now().replace(microsecond=0),
            "dateCreated": datetime.now().replace(microsecond=0),
        }
    ],
    "offerer": [
        {
            "offerer_id": "1",
        },
        {
            "offerer_id": "1",
        },
    ],
    "venue": [{"venue_id": "1", "venue_managing_offerer_id": "1"}],
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "1",
            "booking_is_used": True,
            "booking_is_cancelled": False,
            "booking_amount": 10,
            "booking_quantity": 2,
            "booking_creation_date": datetime.now().replace(microsecond=0),
        },
    ],
    "offer": [
        {
            "offer_id": "1",
            "offer_subcategoryId": "TELECHARGEMENT_MUSIQUE",
            "venue_id": "1",
            "product_id": "1",
            "offer_url": "url",
        },
    ],
    "stock": [
        {"stock_id": "1", "offer_id": "1"},
    ],
    "region_department": [
        {
            "num_dep": 93,
            "dep_name": "Seine-Saint-Denis",
            "region_name": "Île-de-France",
        }
    ],
    "subcategories": [
        {
            "id": "TELECHARGEMENT_MUSIQUE",
            "is_physical_deposit": False,
            "is_digital_deposit": True,
            "is_event": False,
        }
    ],
}

# Test experimentation session should return 2 when user has unused activation booking
# all date should return current date and seniority should return 0
# booking_cnt and no_cancelled_booking should return 1 when user have only one booking and None or 0 for second or
# third booking amount_spent_in_digital_goods should return the amount of the booking when the offer type is MUSIQUE
ENRICHED_USER_DATA_EXPECTED = [
    {
        "user_id": "1",
        "experimentation_session": 2,
        "user_department_code": "93",
        "user_postal_code": "93000",
        "user_activity": "Inactif (ni en emploi ni au chômage), En incapacité de travailler",
        "user_civility": "Mme",
        "user_activation_date": datetime.now().replace(microsecond=0),
        "user_deposit_creation_date": datetime.now().replace(microsecond=0),
        "user_total_deposit_amount": 500,
        "user_current_deposit_type": "GRANT_18",
        "first_connection_date": datetime.now().replace(microsecond=0),
        "first_booking_date": datetime.now().replace(microsecond=0),
        "second_booking_date": None,
        "booking_on_third_product_date": None,
        "booking_cnt": 1,
        "no_cancelled_booking": 1,
        "user_seniority": 0,
        "actual_amount_spent": Decimal("20"),
        "theoretical_amount_spent": Decimal("20"),
        "amount_spent_in_digital_goods": 20.0,
        "amount_spent_in_physical_goods": 0.0,
        "amount_spent_in_outings": 0.0,
        "user_humanized_id": "AE",
        "last_booking_date": datetime.now().replace(microsecond=0),
        "user_region_name": "Île-de-France",
        "booking_creation_date_first": datetime.now().replace(microsecond=0),
        "days_between_activation_date_and_first_booking_date": 0,
        "days_between_activation_date_and_first_booking_paid": 0,
        "first_booking_type": "TELECHARGEMENT_MUSIQUE",
        "first_paid_booking_type": "TELECHARGEMENT_MUSIQUE",
        "cnt_distinct_type_booking": 1,
        "user_is_active": True,
        "user_suspension_reason": None,
        "user_deposit_initial_amount": 500,
        "user_deposit_expiration_date": datetime.now().replace(microsecond=0),
        "user_is_former_beneficiary": True,
        "user_is_current_beneficiary": False,
        "user_age": 18,
        "user_birth_date": datetime.now().replace(microsecond=0),
        "user_school_type": "Lycée agricole",
        "user_subscription_state": "account_created",
    }
]

# Enriched_venue_data => NO DATA (only structure can be tested)
ENRICHED_VENUE_DATA_INPUT = {
    "booking": [
        {
            "booking_id": "1",
            "stock_id": "1",
            "booking_is_cancelled": False,
            "booking_is_used": True,
            "booking_amount": 2,
            "booking_quantity": 1,
            "booking_is_cancelled": False,
            "booking_creation_date": datetime.now().replace(microsecond=0),
        }
    ],
    "favorite": [],
    "offer": [
        {
            "offer_id": "1",
            "venue_id": "1",
            "offer_subcategoryId": "SEANCE_CINE",
            "booking_email": "test@example.com",
            "offer_creation_date": datetime.now().replace(microsecond=0),
        }
    ],
    "enriched_offer_data": [
        {
            "offer_id": "1",
            "venue_id": "1",
            "offer_subcategoryId": "SEANCE_CINE",
            "offer_is_bookable": True,
        }
    ],
    "offerer": [{"offerer_id": "1", "offerer_name": "An offerer"}],
    "payment": [],
    "payment_status": [],
    "stock": [
        {
            "stock_id": "1",
            "offer_id": "1",
        }
    ],
    "venue": [
        {
            "venue_id": "1",
            "venue_public_name": "Venue public name",
            "venue_name": "venue name",
            "venue_booking_email": "venue@example.com",
            "venue_address": "37 rue de la Martinière",
            "venue_latitude": 2.23,
            "venue_longitude": 35.5,
            "venue_department_code": "92",
            "venue_postal_code": "92300",
            "venue_city": "Levallois",
            "venue_siret": "12345678912345",
            "venue_is_virtual": False,
            "venue_is_permanent": True,
            "venue_type_code": "Librairie",
            "venue_managing_offerer_id": "1",
            "venue_creation_date": datetime.now().replace(microsecond=0),
            "venue_label_id": "1",
            "venue_type_id": "1",
            "business_unit_id": "1234",
        }
    ],
    "venue_label": [{"id": "1", "label": "an other label"}],
    "venue_type": [{"id": "1", "label": "a label"}],
    "region_department": [{"num_dep": "92", "region_name": "IDF"}],
}

ENRICHED_VENUE_DATA_EXPECTED = [
    {
        "venue_id": "1",
        "venue_name": "Venue public name",
        "venue_booking_email": "venue@example.com",
        "venue_address": "37 rue de la Martinière",
        "venue_latitude": Decimal("2.23"),
        "venue_longitude": Decimal("35.5"),
        "venue_department_code": "92",
        "venue_postal_code": "92300",
        "venue_city": "Levallois",
        "venue_siret": "12345678912345",
        "venue_is_virtual": False,
        "venue_is_permanent": True,
        "venue_managing_offerer_id": "1",
        "venue_creation_date": datetime.now().replace(microsecond=0),
        "offerer_name": "An offerer",
        "venue_type_label": "Librairie",
        "venue_label": "an other label",
        "total_bookings": 1,
        "non_cancelled_bookings": 1,
        "used_bookings": 1,
        "first_offer_creation_date": datetime.now().replace(microsecond=0),
        "last_offer_creation_date": datetime.now().replace(microsecond=0),
        "first_booking_date": datetime.now().replace(microsecond=0),
        "last_booking_date": datetime.now().replace(microsecond=0),
        "offers_created": 1,
        "venue_bookable_offer_cnt": 1,
        "theoretic_revenue": Decimal("2"),
        "real_revenue": Decimal("2"),
        "venue_humanized_id": "AE",
        "venue_flaskadmin_link": "https://backend.passculture.pro/pc/back-office/venue/edit/?id=1&url=%2Fpc%2Fback-office%2Fvenue%2F",
        "venue_region_name": "IDF",
        "venue_pc_pro_link": "https://passculture.pro/structures/AE/lieux/AE",
        "business_unit_id": "1234",
    }
]

# Enriched_offerer_data => NO DATA (only structure can be tested)
ENRICHED_OFFERER_DATA_INPUT = {
    "booking": [
        {
            "booking_id": "1",
            "booking_creation_date": datetime.now().replace(microsecond=0),
            "stock_id": "1",
            "booking_is_cancelled": False,
            "booking_is_used": True,
            "booking_quantity": 1,
            "booking_amount": 2,
        }
    ],
    "offer": [
        {
            "offer_id": "1",
            "venue_id": "1",
        }
    ],
    "enriched_offer_data": [
        {
            "offer_id": "1",
            "offerer_id": "1",
            "venue_id": "1",
            "offer_is_bookable": True,
        }
    ],
    "offerer": [
        {
            "offerer_id": "1",
            "offerer_postal_code": "973",
            "offerer_name": "An offerer",
            "offerer_siren": "123456789",
            "offerer_creation_date": datetime.now().replace(microsecond=0),
            "offerer_validation_date": datetime.now().replace(microsecond=0),
        }
    ],
    "stock": [
        {
            "stock_id": "1",
            "offer_id": "1",
            "stock_creation_date": datetime.now().replace(microsecond=0),
        }
    ],
    "venue": [
        {
            "venue_id": "1",
            "venue_managing_offerer_id": "1",
        }
    ],
    "venue_label": [],
    "venue_type": [],
    "region_department": [{"num_dep": "973", "region_name": "Guyane"}],
}

ENRICHED_OFFERER_DATA_EXPECTED = [
    {
        "offerer_id": "1",
        "offerer_name": "An offerer",
        "offerer_creation_date": datetime.now().replace(microsecond=0),
        "offerer_validation_date": datetime.now().replace(microsecond=0),
        "first_stock_creation_date": datetime.now().replace(microsecond=0),
        "first_booking_date": datetime.now().replace(microsecond=0),
        "offer_cnt": 1,
        "offerer_bookable_offer_cnt": 1,
        "no_cancelled_booking_cnt": 1,
        "offerer_department_code": "973",
        "offerer_region_name": "Guyane",
        "offerer_siren": "123456789",
        "venue_cnt": 1,
        "venue_with_offer": 1,
        "offerer_humanized_id": "AE",
        "current_year_revenue": Decimal("2"),
    }
]

# Enriched_booking_data => information for one booking not cancelled and used,
# booking is linked with user, venue, offerer and paiement
# venue is linked with venue label and type
ENRICHED_BOOKING_DATA_INPUT = {
    "booking": [
        {
            "booking_id": "1",
            "individual_booking_id": "1",
            "booking_amount": 3,
            "booking_quantity": 1,
            "booking_creation_date": datetime.now().replace(microsecond=0),
            "stock_id": "4",
            "booking_status": "USED",
            "booking_is_cancelled": False,
            "booking_is_used": True,
            "booking_cancellation_date": None,
            "booking_used_date": datetime.now().replace(microsecond=0),
        }
    ],
    "individual_booking": [
        {
            "individual_booking_id": "1",
            "user_id": "13",
            "deposit_id": "18",
        }
    ],
    "deposit": [
        {
            "id": "18",
            "amount": "300",
            "userId": "13",
            "source": "dossier jouve [587030]",
            "dateCreated": datetime.now().replace(microsecond=0),
            "expirationDate": datetime.now().replace(microsecond=0),
            "type": "GRANT_18",
        }
    ],
    "payment": [
        {
            "id": "1",
            "bookingId": "1",
            "author": "michel",
        }
    ],
    "payment_status": [{"id": "1", "paymentId": "1", "status": "SENT"}],
    "stock": [
        {
            "stock_id": "4",
            "offer_id": "2",
            "stock_beginning_date": datetime.now().replace(microsecond=0),
        }
    ],
    "offer": [
        {
            "offer_id": "2",
            "offer_subcategoryId": "ACHAT_INSTRUMENT",
            "offer_name": "An Awesome Offer",
            "venue_id": "8",
        }
    ],
    "venue": [
        {
            "venue_id": "8",
            "venue_public_name": "My Wonderful Venue",
            "venue_name": "My Wonderful Venue",
            "venue_label_id": "15",
            "venue_department_code": 78,
            "venue_managing_offerer_id": "2",
            "venue_type_id": "1",
            "venue_type_code": "Librairie",
        }
    ],
    "offerer": [{"offerer_id": "2", "offerer_name": "Offerer"}],
    "user": [
        {
            "user_id": "13",
            "user_department_code": 68,
            "user_creation_date": datetime.now().replace(microsecond=0),
        }
    ],
    "venue_type": [{"id": "1", "label": "label"}],
    "venue_label": [{"id": "15", "label": "label"}],
    "subcategories": [
        {
            "id": "ACHAT_INSTRUMENT",
            "category_id": "INSTRUMENT",
            "is_physical_deposit": True,
            "is_digital_deposit": False,
            "is_event": False,
        }
    ],
}

# Enriched booking data return data linked to the booking like booking data, offer data, offerer and venue data
# booking has a payment so it is reimbursed
# booking is not cancelled so cancel informations are none
# Offer type is ThingType.Instrument and venue name is not Offre numérique so event is false, digital_good is false and physical good is true
ENRICHED_BOOKING_DATA_EXPECTED = [
    {
        "booking_amount": Decimal("3"),
        "booking_cancellation_date": None,
        "booking_cancellation_reason": None,
        "booking_creation_date": datetime.now().replace(microsecond=0),
        "booking_id": "1",
        "individual_booking_id": "1",
        "booking_intermediary_amount": Decimal("3"),
        "booking_status": "USED",
        "booking_is_cancelled": False,
        "booking_is_used": True,
        "booking_quantity": 1,
        "booking_rank": 1,
        "digital_goods": False,
        "event": False,
        "offer_id": "2",
        "offer_name": "An Awesome Offer",
        "offer_subcategoryId": "ACHAT_INSTRUMENT",
        "offer_category_id": "INSTRUMENT",
        "offerer_id": "2",
        "offerer_name": "Offerer",
        "physical_goods": True,
        "reimbursed": True,
        "same_category_booking_rank": 1,
        "stock_beginning_date": datetime.now().replace(microsecond=0),
        "stock_id": "4",
        "user_creation_date": datetime.now().replace(microsecond=0),
        "user_department_code": "68",
        "user_id": "13",
        "deposit_id": "18",
        "deposit_type": "GRANT_18",
        "venue_department_code": "78",
        "venue_id": "8",
        "venue_label_name": "label",
        "venue_name": "My Wonderful Venue",
        "venue_type_name": "Librairie",
        "booking_used_date": datetime.now().replace(microsecond=0),
    }
]

# Enriched_collective_booking =>
# booking is linked with user, venue, educational_booking, educational_institution and eple
ENRICHED_COLLECTIVE_BOOKING_DATA_INPUT = {
    "booking": [
        {
            "booking_id": "8",
            "booking_amount": 50,
            "collective_booking_id": "8",
            "booking_creation_date": datetime.now().replace(microsecond=0),
            "stock_id": "9",
            "booking_status": "USED",
            "booking_is_cancelled": False,
            "booking_is_used": True,
            "booking_cancellation_date": None,
            "booking_used_date": datetime.now().replace(microsecond=0),
        }
    ],
    "collective_booking": [
        {
            "collective_booking_id": "8",
            "booking_id": "8",
            "collective_booking_collective_stock_id": "9",
            "collective_booking_educational_institution_id": "14",
            "collective_booking_educational_year_id": "1",
            "collective_booking_venue_id": "8",
            "collective_booking_status": "USED_BY_INSTITUTE",
            "collective_booking_confirmation_date": datetime.now().replace(
                microsecond=0
            ),
            "collective_booking_confirmation_limit_date": datetime.now().replace(
                microsecond=0
            ),
            "collective_booking_educational_redactor_id": "1",
        }
    ],
    "educational_institution": [
        {
            "educational_institution_id": "14",
            "educational_institution_institution_id": "14",
        }
    ],
    "collective_offer": [
        {
            "collective_offer_offer_id": "11",
            "collective_offer_subcategory_id": "CINE_PLEIN_AIR",
            "collective_offer_name": "EAC sympa",
            "collective_offer_venue_id": "8",
        }
    ],
    "collective_stock": [
        {
            "collective_stock_stock_id": "9",
            "collective_stock_collective_offer_id": "11",
            "collective_stock_number_of_tickets": 30,
            "collective_stock_beginning_date": datetime.now().replace(microsecond=0),
        }
    ],
    "venue": [
        {
            "venue_id": "8",
            "venue_public_name": "My Wonderful Venue",
            "venue_name": "My Wonderful Venue",
            "venue_label_id": "15",
            "venue_department_code": 78,
            "venue_managing_offerer_id": "2",
            "venue_type_id": "1",
        }
    ],
    "region_department": [
        {
            "num_dep": 78,
            "dep_name": "Yvelines",
            "region_name": "Île-de-France",
        }
    ],
    "eple": [
        {
            "id_etablissement": "14",
            "nom_etablissement": "Mon etablissement",
            "libelle_academie": "Mon academie",
            "code_departement": 78,
        }
    ],
    "offerer": [
        {
            "offerer_id": "2",
            "offerer_name": "Ma structure",
        }
    ],
}

ENRICHED_COLLECTIVE_BOOKING_DATA_EXPECTED = [
    {
        "collective_booking_id": "8",
        "booking_id": "8",
        "collective_offer_id": "11",
        "offer_id": "11",
        "stock_id": "9",
        "collective_stock_id": "9",
        "collective_offer_name": "EAC sympa",
        "collective_offer_subcategory_id": "CINE_PLEIN_AIR",
        "venue_id": "8",
        "venue_name": "My Wonderful Venue",
        "venue_department_code": 78,
        "offerer_id": "2",
        "offerer_name": "Ma structure",
        "booking_amount": 50,
        "number_of_tickets": 30,
        "educational_institution_id": "14",
        "educational_year_id": "1",
        "educational_redactor_id": "1",
        "nom_etablissement": "Mon etablissement",
        "school_department_code": 78,
        "libelle_academie": "Mon academie",
        "collective_booking_creation_date": datetime.now().replace(microsecond=0),
        "collective_booking_status": "USED_BY_INSTITUTE",
        "collective_booking_cancellation_date": None,
        "collective_booking_cancellation_reason": None,
        "collective_booking_confirmation_date": datetime.now().replace(microsecond=0),
        "collective_booking_confirmation_limit_date": datetime.now().replace(
            microsecond=0
        ),
        "collective_booking_used_date": datetime.now().replace(microsecond=0),
        "collective_booking_reimbursement_date": None,
        "collective_booking_rank": 1,
    }
]
