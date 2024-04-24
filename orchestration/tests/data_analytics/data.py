from datetime import datetime

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
