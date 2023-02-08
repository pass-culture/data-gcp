ref_tables = {
    "macro_rayons": {
        "file_type": "csv",
        "schema": {"index": "INTEGER", "macro_rayon": "STRING", "rayon": "STRING"},
    },
    "eac_cash_in": {
        "file_type": "csv",
        "schema": {
            "ministry": "STRING",
            "date_update": "DATE",
            "cash_in": "FLOAT",
        },
    },
    "titelive_isbn_weight_20230208": {
        "file_type": "csv",
        "schema": {
            "EAN13": "STRING",
            "POIDS": "INTEGER",
            "LONGUEUR": "INTEGER",
            "LARGEUR": "INTEGER",
        },
    },
}
