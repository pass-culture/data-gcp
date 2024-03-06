from datetime import datetime

# Enriched_stock_data
ENRICHED_STOCK_DATA_INPUT = {
    "applicative_database_booking": [
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
            "booking_status": "REIMBURSED",
            "booking_used_date": "2019-11-22",
        }
    ],
    "applicative_database_offer": [
        {
            "venue_id": "1",
            "offer_product_id": "1",
            "offer_id": "3",
            "offer_subcategoryId": "SEANCE_CINE",
            "offer_name": "Test",
            "offer_is_active": True,
            "offer_url": None,
            "offer_is_national": False,
            "offer_creation_date": "2019-11-20",
            "offer_is_duo": False,
            "offer_fields_updated": "{}",
        },
        {
            "venue_id": "1",
            "offer_product_id": "2",
            "offer_id": "2",
            "offer_subcategoryId": "LIVRE_PAPIER",
            "offer_name": "Test bis",
            "offer_is_active": True,
            "offer_url": None,
            "offer_is_national": False,
            "offer_creation_date": "2019-11-20",
            "offer_is_duo": False,
            "offer_fields_updated": "{}",
        },
    ],
    "applicative_database_offerer": [
        {
            "offerer_id": "3",
            "offerer_is_active": True,
            "offerer_postal_code": "93100",
            "offerer_city": "Montreuil",
            "offerer_creation_date": "2019-11-20",
            "offerer_name": "Test Offerer",
            "offerer_siren": "123456789",
        }
    ],
    "applicative_database_payment": [
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
    "applicative_database_payment_status": [
        {"paymentId": "1", "id": "1", "date": "2019-01-01", "status": "PENDING"}
    ],
    "applicative_database_product": [
        {
            "id": "1",
            "type": "EventType.CINEMA",
            "thumbCount": "0",
            "name": "Livre",
            "fieldsUpdated": "{}",
            "url": None,
            "isNational": False,
        },
        {
            "id": "1",
            "type": "ThingType.LIVRE_EDITION",
            "thumbCount": "0",
            "name": "Livre",
            "fieldsUpdated": "{}",
            "url": None,
            "isNational": False,
        },
    ],
    "available_stock_information": [
        {"stock_id": "1", "available_stock_information": 8},
        {"stock_id": "2", "available_stock_information": 12},
    ],
    "stock_booking_information": [
        {
            "stock_id": "1",
            "booking_quantity": 2,
            "booking_cancelled": 0,
            "bookings_paid": 2,
        },
        {
            "stock_id": "2",
            "booking_quantity": 0,
            "booking_cancelled": 0,
            "bookings_paid": 0,
        },
    ],
    "cleaned_stock": [
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
            "price_category_id": None,
            "price_category_label_id": None,
            "price_category_label": None,
            "stock_features": None,
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
            "price_category_id": None,
            "price_category_label_id": None,
            "price_category_label": None,
            "stock_features": None,
        },
    ],
    "applicative_database_user": [
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
    "applicative_database_venue": [
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
            "venue_is_permanent": True,
        }
    ],
}

ENRICHED_STOCK_DATA_EXPECTED = [
    {
        "stock_id": "1",
        "offer_id": "3",
        "offer_name": "Test",
        "offerer_id": "3",
        "partner_id": "venue-1",
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
        "price_category_id": None,
        "price_category_label_id": None,
        "price_category_label": None,
        "stock_features": None,
    },
    {
        "stock_id": "2",
        "offer_id": "2",
        "offer_name": "Test bis",
        "offerer_id": "3",
        "partner_id": "venue-1",
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
        "price_category_id": None,
        "price_category_label_id": None,
        "price_category_label": None,
        "stock_features": None,
    },
]

# Offer_moderation =>
# offer moderation is linked with available_stock_information, region_department, applicative_database_venue_label, siren_data, applicative_database_venue_contact and subcategories
OFFER_MODERATION_INPUT = {
    "applicative_database_offer": [
        {
            "offer_id": "1",
            "offer_name": "trois etoiles",
            "offer_subcategoryid": "LIVRE_PAPIER",
            "offer_creation_date": datetime.now().replace(microsecond=0),
            "offer_external_ticket_office_url": "https://www.cab.fa/",
            "offer_id_at_providers": "9782369563235",
            "offer_is_duo": False,
            "offer_is_active": True,
            "offer_validation": "APPROVED",
        }
    ],
    "applicative_database_stock": [
        {
            "offer_id": "1",
            "stock_id": "88",
            "stock_is_soft_deleted": False,
            "stock_booking_limit_date": None,
            "stock_beginning_date": None,
            "stock_quantity": 2,
            "stock_price": 10.00,
        }
    ],
    "available_stock_information": [
        {
            "stock_id": "88",
            "available_stock_information": 1,
        }
    ],
    "applicative_database_booking": [
        {
            "booking_id": "888",
            "stock_id": "88",
            "booking_creation_date": datetime.now().replace(microsecond=0),
            "booking_is_cancelled": False,
        }
    ],
    "subcategories": [
        {
            "id": "LIVRE_PAPIER",
            "category_id": "LIVRE",
            "is_physical_deposit": True,
        }
    ],
    "applicative_database_offer_criterion": [
        {
            "offerid": "1",
            "criterionId": "95",
        }
    ],
    "applicative_database_criterion": [
        {
            "id": "95",
            "name": "livre_tag",
        }
    ],
    "applicative_database_venue": [
        {
            "venue_id": "3",
            "venue_public_name": "My Wonderful Venue",
            "venue_name": "My Wonderful Venue",
            "venue_label_id": "15",
            "venue_department_code": "78",
            "venue_postal_code": "78001",
            "venue_managing_offerer_id": "4",
            "venue_type_code": "Librairie",
            "venue_type_id": "1",
            "venue_booking_email": "venue@example.com",
        }
    ],
    "applicative_database_venue_label": [
        {
            "venue_label_id": "15",
            "venue_label": "Scène nationale",
        }
    ],
    "applicative_database_venue_contact": [
        {
            "venue_id": "3",
            "venue_contact_phone_number": "0303456",
        }
    ],
    "applicative_database_offerer": [
        {
            "offerer_id": "4",
            "offerer_name": "Ma structure",
            "offerer_siren": "1010",
        }
    ],
    "region_department": [
        {
            "num_dep": "78",
            "region_name": "Île-de-France",
        }
    ],
    "siren_data": [
        {
            "siren": "1010",
            "activitePrincipaleUniteLegale": "84.11Z",
        }
    ],
    "applicative_database_offerer_tag_mapping": [
        {
            "offerer_id": "4",
            "tag_id": "1",
        }
    ],
    "applicative_database_offerer_tag": [
        {
            "offerer_tag_id": "1",
            "offerer_tag_label": "Numérique",
        }
    ],
}

OFFER_MODERATION_EXPECTED = [
    {
        "offer_id": "1",
        "offer_name": "trois etoiles",
        "offer_subcategoryid": "LIVRE_PAPIER",
        "category_id": "LIVRE",
        "physical_goods": True,
        "is_book": True,
        "offer_creation_date": datetime.now().replace(microsecond=0),
        "offer_external_ticket_office_url": "https://www.cab.fa/",
        "input_type": "synchro",
        "offer_is_bookable": True,
        "offer_is_duo": False,
        "offer_is_active": True,
        "offer_status": "APPROVED",
        "is_sold_out": False,
        "offerer_id": "4",
        "offerer_name": "Ma structure",
        "venue_id": "3",
        "venue_name": "My Wonderful Venue",
        "venue_public_name": "My Wonderful Venue",
        "region_name": "Île-de-France",
        "venue_department_code": "78",
        "venue_postal_code": "78001",
        "venue_type_label": "Librairie",
        "is_dgca": True,
        "venue_label": "Scène nationale",
        "venue_humanized_id": "AM",
        "venue_booking_email": "venue@example.com",
        "venue_contact_phone_number": "0303456",
        "is_collectivity": True,
        "offer_humanized_id": "AE",
        "passculture_pro_url": "https://passculture.pro/offre/individuelle/AE/informations",
        "webapp_url": "https://passculture.app/offre/1",
        "link_pc_pro": "https://passculture.pro/offres?structure=AQ",
        "first_booking_date": datetime.now().replace(microsecond=0),
        "max_bookings_in_day": 1,
        "cnt_bookings_cancelled": 0,
        "cnt_bookings_confirm": 1,
        "diffdays_creation_firstbooking": 0,
        "stocks_ids": "88",
        "first_stock_beginning_date": None,
        "last_booking_limit_date": None,
        "offer_stock_quantity": 2,
        "available_stock_quantity": 1,
        "fill_rate": 0.5,
        "last_stock_price": 10.00,
        "playlist_tags": "livre_tag",
        "structure_tags": "Numérique",
    }
]
