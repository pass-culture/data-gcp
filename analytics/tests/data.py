from datetime import datetime
from decimal import Decimal

TEST_TABLE_PREFIX = ""

# enriched_offer_data
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
            "offer_type": "EventType.CINEMA",
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
            "venue_id": "2",
            "product_id": "2",
            "offer_id": "4",
            "offer_type": "ThingType.LIVRE_EDITION",
            "offer_name": "RIP Dylan Rieder",
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
        "offer_type": "EventType.CINEMA",
        "offer_creation_date": datetime(2019, 11, 20, 0, 0),
        "offer_is_duo": False,
        "venue_is_virtual": False,
        "physical_goods": False,
        "outing": True,
        "booking_cnt": 2.0,
        "booking_cancelled_cnt": 0.0,
        "booking_confirm_cnt": 0.0,
        "favourite_cnt": 2.0,
        "stock": 10.0,
        "offer_humanized_id": "AM",
        "passculture_pro_url": "https://pro.passculture.beta.gouv.fr/offres/AM",
        "webapp_url": "https://app.passculture.beta.gouv.fr/offre/details/AM",
        "first_booking_cnt": 1,
    },
    {
        "offer_id": "4",
        "offerer_id": "4",
        "offerer_name": "Test Offerer",
        "venue_id": "2",
        "venue_name": "Test Venue",
        "venue_department_code": "93",
        "offer_name": "RIP Dylan Rieder",
        "offer_type": "ThingType.LIVRE_EDITION",
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
        "passculture_pro_url": "https://pro.passculture.beta.gouv.fr/offres/AQ",
        "webapp_url": "https://app.passculture.beta.gouv.fr/offre/details/AQ",
        "first_booking_cnt": None,
    },
]

# enriched_stock_data
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
            "offer_type": "EventType.CINEMA",
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
            "offer_type": "ThingType.LIVRE_EDITION",
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
        "offer_type": "EventType.CINEMA",
        "venue_department_code": None,
        "stock_creation_date": datetime(2019, 11, 1),
        "stock_booking_limit_date": datetime(2019, 11, 23),
        "stock_beginning_date": datetime(2019, 11, 24),
        "available_stock_information": 8,
        "stock_quantity": 10,
        "booking_quantity": 2,
        "booking_cancelled": 0,
        "booking_paid": 2,
    },
    {
        "stock_id": "2",
        "offer_id": "2",
        "offer_name": "Test bis",
        "offerer_id": "3",
        "offer_type": "ThingType.LIVRE_EDITION",
        "venue_department_code": None,
        "stock_creation_date": datetime(2019, 10, 1),
        "stock_booking_limit_date": None,
        "stock_beginning_date": None,
        "available_stock_information": 12,
        "stock_quantity": 12,
        "booking_quantity": 0,
        "booking_cancelled": 0,
        "booking_paid": 0,
    },
]

# enriched_user_data => user 1 is beneficiary and its department has to exist in region_department, has one used booking on digital goods.
# This booking is corresponding to an offer, with an offered, stock and venue
ENRICHED_USER_DATA_INPUT = {
    "user": [
        {
            "user_id": "1",
            "user_is_beneficiary": True,
            "user_department_code": 93,
            "user_postal_code": "93000",
            "user_activity": "Inactif",
            "user_creation_date": datetime.now().replace(microsecond=0),
            "user_has_seen_tutorials": True,
            "user_cultural_survey_filled_date": datetime.now().replace(microsecond=0),
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
            "offer_type": "ThingType.MUSIQUE",
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
}

# test experimentation session should return 2 when user has unused activation booking
# all date should return current date and seniority should return 0
# booking_cnt and no_cancelled_booking should return 1 when user have only one booking and None or 0 for second or third booking
# amount_spent_in_digital_goods should return the amount of the booking when the offer type is MUSIQUE
ENRICHED_USER_DATA_EXPECTED = [
    {
        "user_id": "1",
        "experimentation_session": 2,
        "user_department_code": "93",
        "user_postal_code": "93000",
        "user_activity": "Inactif (ni en emploi ni au chômage), En incapacité de travailler",
        "user_activation_date": datetime.now().replace(microsecond=0),
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
        "first_booking_type": "ThingType.MUSIQUE",
        "first_paid_booking_type": "ThingType.MUSIQUE",
        "cnt_distinct_type_booking": 1,
    }
]

# enriched_venue_data => NO DATA (only structure can be tested)
ENRICHED_VENUE_DATA_INPUT = {
    "booking": [],
    "favorite": [],
    "offer": [],
    "offerer": [],
    "payment": [],
    "payment_status": [],
    "stock": [],
    "venue": [],
    "venue_label": [],
    "venue_type": [],
    "region_department": [],
}

ENRICHED_VENUE_DATA_EXPECTED = [
    "venue_id",
    "venue_name",
    "venue_booking_email",
    "venue_address",
    "venue_latitude",
    "venue_longitude",
    "venue_department_code",
    "venue_postal_code",
    "venue_city",
    "venue_siret",
    "venue_is_virtual",
    "venue_managing_offerer_id",
    "venue_creation_date",
    "offerer_name",
    "venue_type_label",
    "venue_label",
    "total_bookings",
    "non_cancelled_bookings",
    "used_bookings",
    "first_offer_creation_date",
    "last_offer_creation_date",
    "offers_created",
    "theoretic_revenue",
    "real_revenue",
    "venue_humanized_id",
    "venue_flaskadmin_link",
    "venue_region_name",
]

# enriched_offerer_data => NO DATA (only structure can be tested)
ENRICHED_OFFERER_DATA_INPUT = {
    "booking": [],
    "offer": [],
    "offerer": [],
    "stock": [],
    "venue": [],
    "venue_label": [],
    "venue_type": [],
}

ENRICHED_OFFERER_DATA_EXPECTED = [
    "offerer_id",
    "offerer_name",
    "offerer_creation_date",
    "first_stock_creation_date",
    "first_booking_date",
    "offer_cnt",
    "no_cancelled_booking_cnt",
    "offerer_department_code",
    "venue_cnt",
    "venue_with_offer",
    "offerer_humanized_id",
    "current_year_revenue",
]

# enriched_booking_data => information for one booking not cancelled and used,
# booking is linked with user, venue, offerer and paiement
# venue is linked with venue label and type
ENRICHED_BOOKING_DATA_INPUT = {
    "booking": [
        {
            "booking_id": "1",
            "booking_amount": 3,
            "booking_quantity": 1,
            "user_id": "13",
            "booking_creation_date": datetime.now().replace(microsecond=0),
            "stock_id": "4",
            "booking_is_cancelled": False,
            "booking_is_used": True,
            "booking_cancellation_date": None,
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
            "offer_type": "ThingType.INSTRUMENT",
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
        "booking_intermediary_amount": Decimal("3"),
        "booking_is_cancelled": False,
        "booking_is_used": True,
        "booking_quantity": 1,
        "booking_rank": 1,
        "digital_goods": False,
        "event": False,
        "offer_id": "2",
        "offer_name": "An Awesome Offer",
        "offer_type": "ThingType.INSTRUMENT",
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
        "venue_department_code": "78",
        "venue_id": "8",
        "venue_label_name": "label",
        "venue_name": "My Wonderful Venue",
        "venue_type_name": "label",
    }
]
