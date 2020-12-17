from datetime import datetime

# enriched_offer_data
ENRICHED_OFFER_DATA_INPUT = {
    "booking": [
        {"userId": "1", "stockId": "1", "id": "4", "quantity": "2", "dateCreated": "2019-11-20",
         "token": "ABC123", "amount": "0", "isCancelled": False, "isUsed": False, "dateUsed": "2019-11-22"},
    ],
    "favorite": [
        {"id": "1", "offerId": "3", "userId": "1"},
        {"id": "2", "offerId": "4", "userId": "2"},
        {"id": "3", "offerId": "3", "userId": "3"},
    ],
    "offer": [
        {"venueId": "1", "productId": "1", "id": "3", "type": "EventType.CINEMA", "name": "Test",
         "isActive": True, "mediaUrls": '["https://url.test", "https://someurl.test"]', "url": None,
         "isNational": False, "dateCreated": "2019-11-20", "isDuo": False, "fieldsUpdated": "{}"},
        {"venueId": "2", "productId": "2", "id": "4", "type": "ThingType.LIVRE_EDITION", "name": "RIP Dylan Rieder",
         "isActive": True, "mediaUrls": '["https://url.test", "https://someurl.test"]', "url": None,
         "isNational": False, "dateCreated": "2019-11-20", "isDuo": False, "fieldsUpdated": "{}"},
    ],
    "offerer": [
        {"id": "3", "thumbCount": "0", "isActive": True, "postalCode": "93100", "city": "Montreuil",
         "dateCreated": "2019-11-20", "name": "Test Offerer", "siren": "123456789", "fieldsUpdated": "{}"},
        {"id": "4", "siren": "234567890", "thumbCount": "0", "isActive": True, "postalCode": "93100",
         "city": "Montreuil", "dateCreated": "2019-11-20", "name": "Test Offerer", "fieldsUpdated": "{}"},
    ],
    "payment": [
        {"bookingId": "4", "id": "1", "amount": "10", "reimbursementRule": "test", "reimbursementRate": "1",
         "recipientName": "Toto", "recipientSiren": "123456789", "author": "test"},
    ],
    "payment_status": [
        {"paymentId": "1", "id": "1", "date": "2019-01-01", "status": "PENDING"},
    ],
    "product": [
        {"id": "1", "type": "EventType.CINEMA", "thumbCount": "0", "name": "Livre",
         "mediaUrls": '["https://url.test", "https://someurl.test"]', "fieldsUpdated": "{}", "url": None,
         "isNational": False},
        {"id": "2", "type": "ThingType.LIVRE_EDITION", "thumbCount": "0", "name": "Livre",
         "mediaUrls": '["https://url.test", "https://someurl.test"]', "fieldsUpdated": "{}", "url": None,
         "isNational": False},
    ],
    "stock": [
        {"offerId": "3", "id": "1", "dateCreated": "2019-11-01", "quantity": "10", "bookingLimitDatetime": "2019-11-23",
         "beginningDatetime": "2019-11-24", "isSoftDeleted": False, "dateModified": "2019-11-20", "price": "0",
         "fieldsUpdated": "{}"},
        {"offerId": "4", "id": "2", "dateCreated": "2019-10-01", "quantity": "12", "isSoftDeleted": False,
         "dateModified": "2019-11-20", "price": "0", "bookingLimitDatetime": None, "beginningDatetime": None,
         "fieldsUpdated": "{}"},
    ],
    "user": [
        {"id": "1", "email": "test@email.com", "canBookFreeOffers": True, "isAdmin": False, "postalCode": "93100",
         "departementCode": "93", "publicName": "Test", "dateCreated": "2018-11-20", "needsToFillCulturalSurvey": True,
         "culturalSurveyFilledDate": None},
        {"id": "2", "email": "other@test.com", "canBookFreeOffers": True, "isAdmin": False, "postalCode": "93100",
         "departementCode": "93", "publicName": "Test", "dateCreated": "2018-11-20", "needsToFillCulturalSurvey": True,
         "culturalSurveyFilledDate": None},
        {"id": "3", "email": "louie.lopez@test.com", "canBookFreeOffers": True, "isAdmin": False, "postalCode": "93100",
         "departementCode": "93", "publicName": "Test", "dateCreated": "2018-11-20", "needsToFillCulturalSurvey": True,
         "culturalSurveyFilledDate": None},
    ],
    "venue": [
        {"managingOffererId": "3", "id": "1", "siret": "12345678900026", "thumbCount": "0", "name": "Test Venue",
         "postalCode": "93", "city": "Montreuil", "departementCode": "93", "isVirtual": False, "fieldsUpdated": "{}"},
        {"managingOffererId": "4", "id": "2", "siret": "23456789000067", "thumbCount": "0", "name": "Test Venue",
         "postalCode": "93", "city": "Montreuil", "departementCode": "93", "isVirtual": False, "fieldsUpdated": "{}"},
    ],
}
ENRICHED_OFFER_DATA_EXPECTED = [
    {
        "offer_id": "3",
        "identifiant_structure": "3",
        "nom_structure": "Test Offerer",
        "identifiant_lieu": "1",
        "nom_lieu": "Test Venue",
        "departement_lieu": "93",
        "nom_offre": "Test",
        "categorie_offre": "EventType.CINEMA",
        "date_creation_offre": datetime(2019, 11, 20, 0, 0),
        "duo": False,
        "offre_numerique": False,
        "bien_physique": False,
        "sortie": True,
        "nombre_reservations": 2.0,
        "nombre_reservations_annulees": 0.0,
        "nombre_reservations_validees": 0.0,
        "nombre_fois_ou_l_offre_a_ete_mise_en_favoris": 2.0,
        "stock": 10.0,
        "offer_humanized_id": "AM",
        "lien_portail_pro": "https://pro.passculture.beta.gouv.fr/offres/AM",
        "lien_webapp": "https://app.passculture.beta.gouv.fr/offre/details/AM",
    },
    {
        "offer_id": "4",
        "identifiant_structure": "4",
        "nom_structure": "Test Offerer",
        "identifiant_lieu": "2",
        "nom_lieu": "Test Venue",
        "departement_lieu": "93",
        "nom_offre": "RIP Dylan Rieder",
        "categorie_offre": "ThingType.LIVRE_EDITION",
        "date_creation_offre": datetime(2019, 11, 20, 0, 0),
        "duo": False,
        "offre_numerique": False,
        "bien_physique": True,
        "sortie": False,
        "nombre_reservations": 0.0,
        "nombre_reservations_annulees": 0.0,
        "nombre_reservations_validees": 0.0,
        "nombre_fois_ou_l_offre_a_ete_mise_en_favoris": 1.0,
        "stock": 12.0,
        "offer_humanized_id": "AQ",
        "lien_portail_pro": "https://pro.passculture.beta.gouv.fr/offres/AQ",
        "lien_webapp": "https://app.passculture.beta.gouv.fr/offre/details/AQ",
    },
]

# enriched_stock_data
ENRICHED_STOCK_DATA_INPUT = {
    "booking": [
        {"userId": "1", "stockId": "1", "id": "4", "quantity": "2", "dateCreated": "2019-11-20",
         "token": "ABC123", "amount": "0", "isCancelled": False, "isUsed": False, "dateUsed": "2019-11-22"}
    ],
    "offer": [
        {"venueId": "1", "productId": "1", "id": "3", "type": "EventType.CINEMA", "name": "Test",
         "isActive": True, "mediaUrls": '["https://url.test", "https://someurl.test"]', "url": None,
         "isNational": False, "dateCreated": "2019-11-20", "isDuo": False, "fieldsUpdated": "{}"},
        {"venueId": "1", "productId": "2", "id": "2", "type": "ThingType.LIVRE_EDITION", "name": "Test bis",
         "isActive": True, "mediaUrls": '["https://url.test", "https://someurl.test"]', "url": None,
         "isNational": False, "dateCreated": "2019-11-20", "isDuo": False, "fieldsUpdated": "{}"}
    ],
    "offerer": [
        {"id": "3", "thumbCount": "0", "isActive": True, "postalCode": "93100", "city": "Montreuil",
         "dateCreated": "2019-11-20", "name": "Test Offerer", "siren": "123456789", "fieldsUpdated": "{}"}
    ],
    "payment": [
        {"bookingId": "4", "id": "1", "amount": "10", "reimbursementRule": "test", "reimbursementRate": "1",
         "recipientName": "Toto", "recipientSiren": "123456789", "author": "test"}
    ],
    "payment_status": [
        {"paymentId": "1", "id": "1", "date": "2019-01-01", "status": "PENDING"}
    ],
    "product": [
        {"id": "1", "type": "EventType.CINEMA", "thumbCount": "0", "name": "Livre",
         "mediaUrls": '["https://url.test", "https://someurl.test"]', "fieldsUpdated": "{}", "url": None,
         "isNational": False},
        {"id": "1", "type": "ThingType.LIVRE_EDITION", "thumbCount": "0", "name": "Livre",
         "mediaUrls": '["https://url.test", "https://someurl.test"]', "fieldsUpdated": "{}", "url": None,
         "isNational": False},
    ],
    "stock": [
        {"offerId": "3", "id": "1", "dateCreated": "2019-11-01", "quantity": "10", "bookingLimitDatetime": "2019-11-23",
         "beginningDatetime": "2019-11-24", "isSoftDeleted": False, "dateModified": "2019-11-20", "price": "0",
         "fieldsUpdated": "{}"},
        {"offerId": "2", "id": "2", "dateCreated": "2019-10-01", "quantity": "12", "bookingLimitDatetime": None,
         "beginningDatetime": None, "isSoftDeleted": False, "dateModified": "2019-11-20", "price": "0",
         "fieldsUpdated": "{}"}
             ],
    "user": [
        {"id": "1", "email": "test@email.com", "canBookFreeOffers": True, "isAdmin": False, "postalCode": "93100",
         "departementCode": "93", "publicName": "Test", "dateCreated": "2018-11-20", "needsToFillCulturalSurvey": True,
         "culturalSurveyFilledDate": None},
        {"id": "2", "email": "other@test.com", "canBookFreeOffers": True, "isAdmin": False, "postalCode": "93100",
         "departementCode": "93", "publicName": "Test", "dateCreated": "2018-11-20", "needsToFillCulturalSurvey": True,
         "culturalSurveyFilledDate": None}
    ],
    "venue": [
        {"managingOffererId": "3", "id": "1", "siret": None, "thumbCount": "0", "name": "Test Venue",
         "postalCode": None, "city": None, "departementCode": None, "isVirtual": True, "fieldsUpdated": "{}"}
    ],
}

ENRICHED_STOCK_DATA_EXPECTED = [
    {
        "stock_id": "1",
        "offer_id": "3",
        "nom_offre": "Test",
        "offerer_id": "3",
        "type_d_offre": "EventType.CINEMA",
        "departement": None,
        "date_creation_du_stock": datetime(2019, 11, 1),
        "date_limite_de_reservation": datetime(2019, 11, 23),
        "date_debut_de_l_evenement": datetime(2019, 11, 24),
        "stock_disponible_reel": 8.0,
        "stock_disponible_brut_de_reservations": 10.0,
        "nombre_total_de_reservations": 2.0,
        "nombre_de_reservations_annulees": 0.0,
        "nombre_de_reservations_ayant_un_paiement": 2.0
    },
    {
        "stock_id": "2",
        "offer_id": "2",
        "nom_offre": "Test bis",
        "offerer_id": "3",
        "type_d_offre": "ThingType.LIVRE_EDITION",
        "departement": None,
        "date_creation_du_stock": datetime(2019, 10, 1),
        "date_limite_de_reservation": None,
        "date_debut_de_l_evenement": None,
        "stock_disponible_reel": 12.0,
        "stock_disponible_brut_de_reservations": 12.0,
        "nombre_total_de_reservations": 0.0,
        "nombre_de_reservations_annulees": 0.0,
        "nombre_de_reservations_ayant_un_paiement": 0.0
    },
]

# enriched_user_data => NO DATA (only structure can be tested)
ENRICHED_USER_DATA_INPUT = {
    "booking": [],
    "offer": [],
    "stock": [],
    "user": [],
}

ENRICHED_USER_DATA_EXPECTED = [
    "user_id",
    "vague_experimentation",
    "departement",
    "code_postal",
    "statut",
    "date_activation",
    "date_premiere_connexion",
    "date_premiere_reservation",
    "date_deuxieme_reservation",
    "date_premiere_reservation_dans_3_categories_differentes",
    "nombre_reservations_totales",
    "nombre_reservations_non_annulees",
    "anciennete_en_jours",
    "montant_reel_depense",
    "montant_theorique_depense",
    "depenses_numeriques",
    "depenses_physiques",
    "depenses_sorties",
    "user_humanized_id",
    "date_derniere_reservation",
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
}

ENRICHED_VENUE_DATA_EXPECTED = [
    "venue_id",
    "nom_du_lieu",
    "email",
    "adresse",
    "latitude",
    "longitude",
    "departement",
    "code_postal",
    "ville",
    "siret",
    "lieu_numerique",
    "identifiant_de_la_structure",
    "nom_de_la_structure",
    "type_de_lieu",
    "label_du_lieu",
    "nombre_total_de_reservations",
    "nombre_de_reservations_non_annulees",
    "nombre_de_reservations_validees",
    "date_de_creation_de_la_premiere_offre",
    "date_de_creation_de_la_derniere_offre",
    "nombre_offres_creees",
    "chiffre_affaires_theorique_realise",
    "chiffre_affaires_reel_realise",
    "venue_humanized_id",
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
    "nom",
    "date_de_creation",
    "date_de_creation_du_premier_stock",
    "date_de_premiere_reservation",
    "nombre_offres",
    "nombre_de_reservations_non_annulees",
    "departement",
    "nombre_lieux",
    "nombre_de_lieux_avec_offres",
    "offerer_humanized_id",
]
