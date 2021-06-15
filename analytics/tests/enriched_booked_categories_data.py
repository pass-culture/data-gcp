# Enriched_booked_categories
ENRICHED_BOOKED_CATEGORIES_DATA_INPUT = {
    "booking": [],
    "stock": [],
    "offer": [],
    "venue": [],
}

ENRICHED_BOOKED_CATEGORIES_DATA_EXPECTED = [
    "user_id",
    "audiovisuel",
    "cinema",
    "instrument",
    "jeux_video",
    "livre_numerique",
    "livre_papier",
    "musee_patrimoine",
    "musique_live",
    "musique_cd_vynils",
    "musique_numerique",
    "pratique_artistique",
    "spectacle_vivant",
]

ENRICHED_BOOKED_CATEGORIES_DATA_AUDIOVISUEL_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        }
    ],
    "stock": [{"stock_id": "1", "offer_id": "1"}],
    "offer": [{"offer_id": "1", "offer_type": "ThingType.AUDIOVISUEL"}],
    "venue": [],
}

ENRICHED_BOOKED_CATEGORIES_DATA_AUDIOVISUEL_EXPECTED = [
    {
        "audiovisuel": True,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
]

ENRICHED_BOOKED_CATEGORIES_DATA_CINEMA_INPUT = {
    "booking": [
        {"user_id": "1", "stock_id": "1", "booking_id": "1"},
        {"user_id": "3", "stock_id": "2", "booking_id": "2"},
        {"user_id": "6", "stock_id": "3", "booking_id": "3"},
    ],
    "stock": [
        {"stock_id": "1", "offer_id": "1"},
        {"stock_id": "2", "offer_id": "2"},
        {"stock_id": "3", "offer_id": "3"},
    ],
    "offer": [
        {"offer_id": "1", "offer_type": "EventType.CINEMA"},
        {"offer_id": "2", "offer_type": "ThingType.CINEMA_ABO"},
        {"offer_id": "3", "offer_type": "ThingType.CINEMA_CARD"},
    ],
    "venue": [],
}

ENRICHED_BOOKED_CATEGORIES_DATA_CINEMA_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": True,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
    {
        "audiovisuel": False,
        "cinema": True,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "3",
    },
    {
        "audiovisuel": False,
        "cinema": True,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "6",
    },
]


ENRICHED_BOOKED_CATEGORIES_DATA_INTRUMENT_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        }
    ],
    "stock": [{"stock_id": "1", "offer_id": "1"}],
    "offer": [{"offer_id": "1", "offer_type": "ThingType.INSTRUMENT"}],
    "venue": [],
}

ENRICHED_BOOKED_CATEGORIES_DATA_INTRUMENT_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": True,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
]

ENRICHED_BOOKED_CATEGORIES_DATA_JEUX_VIDEO_INPUT = {
    "booking": [
        {"user_id": "1", "stock_id": "1", "booking_id": "4"},
        {"user_id": "10", "stock_id": "2", "booking_id": "5"},
    ],
    "stock": [
        {"stock_id": "1", "offer_id": "1"},
        {"stock_id": "2", "offer_id": "2"},
    ],
    "offer": [
        {"offer_id": "1", "offer_type": "ThingType.JEUX_VIDEO"},
        {"offer_id": "2", "offer_type": "ThingType.JEUX_VIDEO_ABO"},
    ],
    "venue": [],
}

ENRICHED_BOOKED_CATEGORIES_DATA_JEUX_VIDEO_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": True,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": True,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "10",
    },
]

ENRICHED_BOOKED_CATEGORIES_DATA_LIVRE_NUM_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        }
    ],
    "stock": [{"stock_id": "1", "offer_id": "1"}],
    "offer": [
        {
            "offer_id": "1",
            "offer_type": "ThingType.LIVRE_EDITION",
            "venue_id": "1",
        }
    ],
    "venue": [{"venue_id": "1", "venue_is_virtual": True}],
}

ENRICHED_BOOKED_CATEGORIES_DATA_LIVRE_NUM_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": True,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
]

ENRICHED_BOOKED_CATEGORIES_DATA_LIVRE_PAPIER_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        }
    ],
    "stock": [{"stock_id": "1", "offer_id": "1"}],
    "offer": [
        {
            "offer_id": "1",
            "offer_type": "ThingType.LIVRE_EDITION",
            "venue_id": "1",
        }
    ],
    "venue": [{"venue_id": "1", "venue_is_virtual": False}],
}

ENRICHED_BOOKED_CATEGORIES_DATA_LIVRE_PAPIER_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": True,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
]

ENRICHED_BOOKED_CATEGORIES_DATA_MUSEE_PATRIMOINE_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        },
        {
            "user_id": "2",
            "stock_id": "2",
            "booking_id": "5",
        },
    ],
    "stock": [
        {"stock_id": "1", "offer_id": "1"},
        {"stock_id": "2", "offer_id": "2"},
    ],
    "offer": [
        {"offer_id": "1", "offer_type": "EventType.MUSEES_PATRIMOINE"},
        {"offer_id": "2", "offer_type": "ThingType.MUSEES_PATRIMOINE_ABO"},
    ],
    "venue": [],
}

ENRICHED_BOOKED_CATEGORIES_DATA_MUSEE_PATRIMOINE_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": True,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": True,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "2",
    },
]


ENRICHED_BOOKED_CATEGORIES_DATA_MUSIQUE_LIVE_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        },
        {
            "user_id": "2",
            "stock_id": "2",
            "booking_id": "4",
        },
    ],
    "stock": [
        {"stock_id": "1", "offer_id": "1"},
        {"stock_id": "2", "offer_id": "2"},
    ],
    "offer": [
        {"offer_id": "1", "offer_type": "EventType.MUSIQUE"},
        {"offer_id": "2", "offer_type": "ThingType.MUSIQUE_ABO"},
    ],
    "venue": [],
}

ENRICHED_BOOKED_CATEGORIES_DATA_MUSIQUE_LIVE_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": True,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": True,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "2",
    },
]

ENRICHED_BOOKED_CATEGORIES_DATA_MUSIQUE_CD_VYNILS_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        }
    ],
    "stock": [{"stock_id": "1", "offer_id": "1"}],
    "offer": [
        {
            "offer_id": "1",
            "offer_type": "ThingType.MUSIQUE",
            "venue_id": "1",
        }
    ],
    "venue": [{"venue_id": "1", "venue_is_virtual": False}],
}

ENRICHED_BOOKED_CATEGORIES_DATA_MUSIQUE_CD_VYNILS_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": True,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
]

ENRICHED_BOOKED_CATEGORIES_DATA_MUSIQUE_NUMERIQUE_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        }
    ],
    "stock": [{"stock_id": "1", "offer_id": "1"}],
    "offer": [
        {
            "offer_id": "1",
            "offer_type": "ThingType.MUSIQUE",
            "venue_id": "1",
        }
    ],
    "venue": [{"venue_id": "1", "venue_is_virtual": True}],
}

ENRICHED_BOOKED_CATEGORIES_DATA_MUSIQUE_NUMERIQUE_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": True,
        "pratique_artistique": False,
        "spectacle_vivant": False,
        "user_id": "1",
    },
]


ENRICHED_BOOKED_CATEGORIES_DATA_PRATIQUE_ARTISTIQUE_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        },
        {
            "user_id": "2",
            "stock_id": "2",
            "booking_id": "5",
        },
    ],
    "stock": [
        {"stock_id": "1", "offer_id": "1"},
        {"stock_id": "2", "offer_id": "2"},
    ],
    "offer": [
        {"offer_id": "1", "offer_type": "EventType.PRATIQUE_ARTISTIQUE"},
        {"offer_id": "2", "offer_type": "ThingType.PRATIQUE_ARTISTIQUE"},
    ],
    "venue": [],
}

ENRICHED_BOOKED_CATEGORIES_DATA_PRATIQUE_ARTISTIQUE_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": True,
        "spectacle_vivant": False,
        "user_id": "1",
    },
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": True,
        "spectacle_vivant": False,
        "user_id": "2",
    },
]


ENRICHED_BOOKED_CATEGORIES_DATA_SPECTACLE_VIVANT_INPUT = {
    "booking": [
        {
            "user_id": "1",
            "stock_id": "1",
            "booking_id": "4",
        },
        {
            "user_id": "2",
            "stock_id": "2",
            "booking_id": "5",
        },
    ],
    "stock": [
        {"stock_id": "1", "offer_id": "1"},
        {"stock_id": "2", "offer_id": "2"},
    ],
    "offer": [
        {"offer_id": "1", "offer_type": "EventType.SPECTACLE_VIVANT"},
        {"offer_id": "2", "offer_type": "ThingType.SPECTACLE_VIVANT_ABO"},
    ],
    "venue": [],
}

ENRICHED_BOOKED_CATEGORIES_DATA_SPECTACLE_VIVANT_EXPECTED = [
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": True,
        "user_id": "1",
    },
    {
        "audiovisuel": False,
        "cinema": False,
        "instrument": False,
        "jeux_video": False,
        "livre_numerique": False,
        "livre_papier": False,
        "musee_patrimoine": False,
        "musique_cd_vynils": False,
        "musique_live": False,
        "musique_numerique": False,
        "pratique_artistique": False,
        "spectacle_vivant": True,
        "user_id": "2",
    },
]
