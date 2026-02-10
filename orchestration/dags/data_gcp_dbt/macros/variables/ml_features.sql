-- This macro defines the configuration for semantic embedding features used in
-- machine learning models.
-- It categorizes features into "embedding_features" and "extra_data" for both "offer"
-- and "offer_metadata" entities.
{% macro get_semantic_embedding_feature_config() %}
    {% set config = {
        "offer": {
            "embedding_features": [
                "offer_name",
                "offer_description",
                "titelive_gtl_id",
                "author",
                "performer",
            ],
            "extra_data": [
                "offer_creation_date",
                "offer_subcategory_id",
                "offer_category_id",
                "offer_type_domain",
            ],
        },
        "offer_metadata": {
            "embedding_features": [
                "image_url",
                "gtl_label_level_1",
                "gtl_label_level_2",
                "gtl_label_level_3",
                "gtl_label_level_4",
                "offer_type_labels",
            ],
            "extra_data": [
                "search_group_name",
                "offer_type_id",
                "offer_sub_type_id",
                "gtl_type",
                "offer_type_label",
                "offer_sub_type_label",
                "offer_video_url",
            ],
        },
    } %}
    {{ return(config) }}
{% endmacro %}
