{% macro get_categories() %}
    {#
        Offer categories for diversity and finance reporting.
        Returns: Array of objects with 'name' (category ID) and 'value_expr' (display name) keys.

        Note: Standardized on 'value_expr' key for consistency across all uses.
    #}
    {% set categories = [
        {"name": "LIVRE", "value_expr": "livre"},
        {"name": "CINEMA", "value_expr": "cinema"},
        {"name": "MUSIQUE_LIVE", "value_expr": "concert"},
        {"name": "SPECTACLE", "value_expr": "spectacle_vivant"},
        {"name": "MUSEE", "value_expr": "musee"},
        {"name": "PRATIQUE_ART", "value_expr": "pratique_artistique"},
        {"name": "INSTRUMENT", "value_expr": "instrument"},
        {"name": "LIVRE", "value_expr": "livre"},
        {"name": "CINEMA", "value_expr": "cinema"},
        {"name": "MUSIQUE_LIVE", "value_expr": "concert"},
        {"name": "SPECTACLE", "value_expr": "spectacle_vivant"},
        {"name": "MUSEE", "value_expr": "musee"},
        {"name": "PRATIQUE_ART", "value_expr": "pratique_artistique"},
        {"name": "INSTRUMENT", "value_expr": "instrument"},
        {"name": "BEAUX_ARTS", "value_expr": "beaux_arts"},
        {"name": "CARTE_JEUNES", "value_expr": "cartes_jeunes"},
        {"name": "CONFERENCE", "value_expr": "conference"},
        {"name": "FILM", "value_expr": "film"},
        {"name": "JEU", "value_expr": "jeu"},
        {"name": "MUSIQUE_ENREGISTREE", "value_expr": "musique_enregistree"},
        {"name": "MEDIA", "value_expr": "media"},
        {"name": "TECHNIQUE", "value_expr": "technique"},
    ] %}
    {{ return(categories) }}
{% endmacro %}
