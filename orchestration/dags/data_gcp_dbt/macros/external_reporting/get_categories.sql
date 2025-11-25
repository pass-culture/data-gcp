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
    ] %}
    {{ return(categories) }}
{% endmacro %}
