{% macro titelive_product_setup() %}
    {% set provider_ids = ["9", "16", "17", "19", "20", "1082", "2156", "2190"] %}
    {% set path_code_support = "$.article[0].codesupport" %}

    {% set common_fields_struct = [
        {"json_path": "$.titre", "alias": "title"},
        {"json_path": "$.article[0].resume", "alias": "description"},
        {"json_path": path_code_support, "alias": "support_code"},
        {
            "json_path": "$.article[0].dateparution",
            "alias": "publication_date",
        },
        {"json_path": "$.article[0].editeur", "alias": "publisher"},
        {
            "json_path": "$.article[0].gtl",
            "alias": "gtl",
            "cast_type": "JSON",
        },
        {
            "json_path": "$.article[0].prix",
            "alias": "price",
            "cast_type": "NUMERIC",
        },
        {
            "json_path": "$.article[0].image",
            "alias": "image",
            "cast_type": "INT64",
        },
        {
            "json_path": "$.article[0].image_4",
            "alias": "image_4",
            "cast_type": "INT64",
        },
    ] %}

    {% set specific_paper_fields_struct = [
        {
            "json_path": "$.article[0].id_lectorat",
            "alias": "readership_id",
            "cast_type": "INT64",
            "final_cast_type": "STRING",
        },
        {"json_path": "$.article[0].langueiso", "alias": "language_iso"},
        {
            "json_path": "$.article[0].taux_tva",
            "alias": "vat_rate",
            "cast_type": "FLOAT64",
            "final_cast_type": "STRING",
        },
        {
            "json_path": "$.auteurs_multi",
            "alias": "multiple_authors",
            "cast_type": "JSON",
        },
        {
            "json_path": "$.article[0].contributor",
            "alias": "contributor",
            "cast_type": "JSON",
        },
        {
            "json_path": "$.article[0].serie",
            "alias": "series",
            "null_when_equal": "Non précisée",
        },
        {
            "json_path": "$.article[0].idserie",
            "alias": "series_id",
            "null_when_equal": "0",
        },
    ] %}

    {% set specific_music_fields_struct = [
        {"json_path": "$.article[0].artiste", "alias": "artist"},
        {"json_path": "$.article[0].label", "alias": "music_label"},
        {"json_path": "$.article[0].compositeur", "alias": "composer"},
        {"json_path": "$.article[0].interprete", "alias": "performer"},
        {
            "json_path": "$.article[0].nb_galettes",
            "alias": "nb_discs",
            "cast_type": "INT64",
            "final_cast_type": "STRING",
        },
        {"json_path": "$.article[0].commentaire", "alias": "comment"},
        {
            "json_path": "$.article[0].contenu_explicite",
            "alias": "explicit_content",
            "cast_type": "INT64",
        },
        {
            "json_path": "$.article[0].dispo",
            "alias": "availability",
            "cast_type": "INT64",
        },
        {"json_path": "$.article[0].distributeur", "alias": "distributor"},
    ] %}

    {% set products = {
        "paper": {
            "payload_type": "paper",
            "support_code_pattern": "R'[a-zA-Z]'",
            "specific_fields": specific_paper_fields_struct,
        },
        "music": {
            "payload_type": "music",
            "support_code_pattern": "R'[0-9]'",
            "specific_fields": specific_music_fields_struct,
        },
    } %}

    {{
        return(
            {
                "provider_ids": provider_ids,
                "path_code_support": path_code_support,
                "common_fields_struct": common_fields_struct,
                "specific_paper_fields_struct": specific_paper_fields_struct,
                "specific_music_fields_struct": specific_music_fields_struct,
                "products": products,
            }
        )
    }}
{% endmacro %}
