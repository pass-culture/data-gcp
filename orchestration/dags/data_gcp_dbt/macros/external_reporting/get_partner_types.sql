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
            "name": "tag_cinemas_art_et_essai",
            "label": "Cinémas Art et Essai",
            "condition": "venue_tag_name IN (\"Cinéma d'art et d'essai\")",
        },
        {
            "name": "librairies",
            "label": "Librairies",
            "condition": "partner_type IN ('Librairie', 'Magasin de grande distribution')",
        },
        {
            "name": "tag_librairies_lir",
            "label": "Librairies avec le label LIR",
            "condition": "partner_type IN ('Librairie', 'Magasin de grande distribution') AND venue_tag_name IN ('LIR')",
        },
        {
            "name": "spectacle_vivant",
            "label": "Spectacle Vivant",
            "condition": "partner_type IN ('Spectacle vivant')",
        },
        {
            "name": "tag_spectacle_vivant_labels",
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
            "name": "tag_musee_labels",
            "label": "Musées avec Labels",
            "condition": "partner_type IN ('Musée') AND venue_tag_name IN ('MdF')",
        },
        {
            "name": "arts_visuels_plastiques_galeries",
            "label": "Arts visuels, arts plastiques et galeries",
            "condition": "partner_type IN ('Arts visuels, arts plastiques et galeries')",
        },
        {
            "name": "autre",
            "label": "Autres partenaires culturels",
            "condition": "partner_type IN ('Autre')",
        },
        {
            "name": "bibliotheques_ou_mediatheques",
            "label": "Bibliothèques ou médiathèques",
            "condition": "partner_type IN ('Bibliothèque ou médiathèques')",
        },
        {
            "name": "centre_culturel",
            "label": "Centres culturels",
            "condition": "partner_type IN ('Centre culturel')",
        },
        {
            "name": "pratique_art",
            "label": "Pratique artistique",
            "condition": "partner_type IN ('Cours et pratique artistiques')",
        },
        {
            "name": "culture_scientifique",
            "label": "Culture scientifique",
            "condition": "partner_type IN ('Culture scientifique')",
        },
        {
            "name": "festival",
            "label": "Festivals",
            "condition": "partner_type IN ('Festival')",
        },
        {
            "name": "jeux_videos",
            "label": "Jeux vidéo",
            "condition": "partner_type IN ('Jeux / Jeux vidéos')",
        },
        {
            "name": "magasins_arts_creatifs",
            "label": "Magasin arts créatifs",
            "condition": "partner_type IN ('Magasin arts créatifs')",
        },
        {
            "name": "musique_disquaire",
            "label": "Musique - Disquaires",
            "condition": "partner_type IN ('Musique - Disquaire')",
        },
        {
            "name": "musique_instrument",
            "label": "Musique - Magasins d'instruments",
            "condition": 'partner_type IN ("Musique - Magasin d’instruments")',
        },
        {
            "name": "offre_numerique",
            "label": "Offre numérique",
            "condition": "partner_type IN ('Offre numérique')",
        },
        {
            "name": "patrimoine_tourisme",
            "label": "Patrimoine et tourisme",
            "condition": "partner_type IN ('Patrimoine et tourisme')",
        },
    ] %}
    {{ return(partner_types) }}
{% endmacro %}
