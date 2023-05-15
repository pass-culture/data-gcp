default_config = {
    "features_to_extract_embedding": [
        {"name": "offer_name", "type": "text"},
        {"name": "offer_description", "type": "text"},
        {"name": "offer_image", "type": "image"},
    ],
    "preprocess_features_type": {
        "text_features": [
            "offer_name",
            "offer_description",
            "rayon",
            "macro_rayon",
            "type",
            "subtype",
        ],
        "numerical_features": ["stock", "stock_price"],
        "boolean_features": ["outling", "physical_goods"],
    },
    "catboost_features_types": {
        "cat_features": [
            "offer_subcategoryid",
            "rayon",
            "macro_rayon",
            "type",
            "subtype",
        ],
        "text_features": ["offer_name", "offer_description"],
        "embedding_features": [
            "offer_image_embedding",
            "offer_name_embedding",
            "offer_description_embedding",
        ],
    },
}
