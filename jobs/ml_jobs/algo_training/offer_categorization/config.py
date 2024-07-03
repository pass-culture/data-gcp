features = {
    "default": {
        "preprocess_features_type": {
            "text_features": ["offerer_name", "venue_type_label"],
            "embedding_features": ["name_embedding", "description_embedding"],
        },
        "catboost_features_types": {
            "cat_features": ["venue_type_label"],
            "text_features": ["offerer_name"],
            "embedding_features": ["name_embedding", "description_embedding"],
        },
    }
}
