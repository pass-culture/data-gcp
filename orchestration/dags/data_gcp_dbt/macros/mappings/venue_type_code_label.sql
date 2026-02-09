{% macro venue_type_code_label() %}
    {% set venue_type_code_label_mapping = [
        {"name": "ADMINISTRATIVE", "label": "Lieu administratif"},
        {"name": "DIGITAL", "label": "Offre numérique"},
        {"name": "BOOKSTORE", "label": "Librairie"},
        {"name": "PERFORMING_ARTS", "label": "Spectacle vivant"},
        {"name": "ARTISTIC_COURSE", "label": "Cours et pratique artistiques"},
        {"name": "MOVIE", "label": "Cinéma - Salle de projections"},
        {"name": "OTHER", "label": "Autre"},
        {"name": "CONCERT_HALL", "label": "Musique - Salle de concerts"},
        {"name": "MUSEUM", "label": "Musée"},
        {"name": "CULTURAL_CENTRE", "label": "Centre culturel"},
        {"name": "PATRIMONY_TOURISM", "label": "Patrimoine et tourisme"},
        {"name": "FESTIVAL", "label": "Festival"},
        {
            "name": "MUSICAL_INSTRUMENT_STORE",
            "label": "Musique - Magasin d’instruments",
        },
        {"name": "LIBRARY", "label": "Bibliothèque ou médiathèque"},
        {
            "name": "VISUAL_ARTS",
            "label": "Arts visuels, arts plastiques et galeries",
        },
        {"name": "GAMES", "label": "Jeux / Jeux vidéos"},
        {"name": "CREATIVE_ARTS_STORE", "label": "Magasin arts créatifs"},
        {"name": "RECORD_STORE", "label": "Musique - Disquaire"},
        {"name": "SCIENTIFIC_CULTURE", "label": "Culture scientifique"},
        {"name": "TRAVELING_CINEMA", "label": "Cinéma itinérant"},
        {
            "name": "DISTRIBUTION_STORE",
            "label": "Magasin de grande distribution",
        },
    ] %}
    {{ return(venue_type_code_label_mapping) }}
{% endmacro %}
