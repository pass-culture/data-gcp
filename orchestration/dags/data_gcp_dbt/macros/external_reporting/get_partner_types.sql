{% macro get_partner_types() %}
    {#
        Cultural partner types with SQL filter conditions.
        Returns: Array of partner type objects with 'name', 'label', and 'condition' keys.
    #}
    {% set partner_types = [
        {
            "name": "cinemas",
            "label": "Cinémas",
            "condition": "partner_type IN ('Cinéma - Salle de projections', 'Cinéma itinérant') OR venue_tag_name IN (\"Cinéma d'art et d'essai\")",
        },
        {
            "name": "cinemas_art_et_essai",
            "label": "Cinémas Art et Essai",
            "condition": "venue_tag_name IN (\"Cinéma d'art et d'essai\")",
        },
        {
            "name": "librairies",
            "label": "Librairies",
            "condition": "partner_type IN ('Librairie', 'Magasin de grande distribution')",
        },
        {
            "name": "spectacle_vivant",
            "label": "Spectacle Vivant",
            "condition": "partner_type IN ('Spectacle vivant')",
        },
        {
            "name": "spectacle_vivant_labels",
            "label": "Spectacle Vivant avec Labels",
            "condition": "partner_type IN ('Spectacle vivant') AND venue_tag_name IN ('CCN','CDCN','CDN','CNAREP','SCIN','Scène nationale','Théâtre lyrique','Théâtres nationaux')",
        },
        {
            "name": "musique_live",
            "label": "Musique Live",
            "condition": "partner_type IN ('Musique - Salle de concerts', 'Festival')",
        },
        {
            "name": "musee",
            "label": "Musées",
            "condition": "partner_type IN ('Musée')",
        },
        {
            "name": "musee_labels",
            "label": "Musées avec Labels",
            "condition": "partner_type IN ('Musée') AND venue_tag_name IN ('MdF')",
        },
    ] %}
    {{ return(partner_types) }}
{% endmacro %}
