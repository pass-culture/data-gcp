configs = {
    "default": {
        "features_to_extract_embedding": [
            {"name": "offer_name", "type": "text"},
            {"name": "offer_description", "type": "text"},
            {"name": "offer_image", "type": "image"},
        ],
        "pre_trained_model_for_embedding_extraction": {
            "image": "clip-ViT-B-32",
            "text": "sentence-transformers/all-MiniLM-L6-v2",
        },
        "preprocess_features_type": {
            "text_features": [
                "offer_name",
                "offer_description",
                "rayon",
                "macro_rayon",
            ],
            "numerical_features": ["stock", "stock_price"],
        },
        "catboost_features_types": {
            "cat_features": [
                "offer_subcategoryid",
                "rayon",
                "macro_rayon",
            ],
            "text_features": ["offer_name", "offer_description"],
            "embedding_features": [
                "offer_image_embedding",
                "offer_name_embedding",
                "offer_description_embedding",
            ],
        },
    },
}
