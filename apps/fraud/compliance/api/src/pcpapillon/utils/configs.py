configs = {
    "API": {
        "default": {
            "features_to_extract_embedding": [
                {"name": "offer_name", "type": "text"},
                {"name": "offer_description", "type": "text"},
                {"name": "image_url", "type": "image"},
                {"name": "semantic_content", "type": "text"},
            ],
            "preprocess_features_type": {
                "text_features": [
                    "offer_name",
                    "offer_description",
                    "rayon",
                    "macro_rayon",
                    # "image_url"
                ],
                "numerical_features": ["stock_price"],
                "macro_text": [
                    "offer_name",
                    "offer_description",
                    "offer_type_label",
                    "offer_sub_type_label",
                    "author",
                    "performer",
                ],
            },
        }
    },
    "model": {
        "default": {
            "pre_trained_model_for_embedding_extraction": {
                "image": "clip-ViT-B-32",
                "text": "sentence-transformers/clip-ViT-B-32-multilingual-v1",
            },
            "catboost_features_types": {
                "cat_features": [
                    "offer_subcategoryid",
                    "rayon",
                    "macro_rayon",
                ],
                "text_features": ["offer_name", "offer_description"],
                "numerical_features": ["stock_price"],
                "embedding_features": ["image_embedding"],
            },
        },
        "semantic_content": {
            "pre_trained_model_for_embedding_extraction": {
                "image": "clip-ViT-B-32",
                "text": "sentence-transformers/clip-ViT-B-32-multilingual-v1",
            },
            "catboost_features_types": {
                "cat_features": [
                    "offer_subcategoryid",
                    "rayon",
                    "macro_rayon",
                ],
                "text_features": [],
                "numerical_features": ["stock_price"],
                "embedding_features": ["image_embedding", "semantic_content_embedding"],
            },
        },
    },
}
