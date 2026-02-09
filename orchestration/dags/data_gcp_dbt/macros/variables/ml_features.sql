{% macro get_offer_features() %}
    {{
        return(
            [
                "offer_name",
                "offer_description",
                "image_url",
                "titelive_gtl_id",
                "gtl_label_level_1",
                "gtl_label_level_2",
                "gtl_label_level_3",
                "gtl_label_level_4",
                "offer_type_labels",
                "author",
                "performer",
            ]
        )
    }}
{% endmacro %}

{% macro get_offer_metadata_features() %}
    {{
        return(
            [
                "offer_creation_date",
                "offer_subcategory_id",
                "offer_category_id",
                "offer_type_domain",
                "search_group_name",
                "offer_type_id",
                "offer_sub_type_id",
                "gtl_type",
                "offer_type_label",
                "offer_sub_type_label",
            ]
        )
    }}
{% endmacro %}
