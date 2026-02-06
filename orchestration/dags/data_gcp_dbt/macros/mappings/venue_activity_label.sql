{% macro venue_activity_label() %}
{% set venue_activity_label_mapping = [
        {"name": "ART_GALLERY", "label": "Galerie d’art"},
        {"name": "ART_SCHOOL", "label": "Conservatoire ou école d’arts"},
        {"name": "ARTISTIC_COMPANY", "label": "Compagnie artistique"},
        {"name": "ARTS_CENTRE", "label": "Centre d’arts"},
        {
            "name": "ARTS_EDUCATION",
            "label": "Formation ou enseignement artistique",
        },
        {"name": "BOOKSTORE", "label": "Librairie"},
        {"name": "CINEMA", "label": "Cinéma"},
        {"name": "COMMUNITY_CENTRE", "label": "Centre socio-culturel"},
        {"name": "CREATIVE_ARTS_STORE", "label": "Magasin d’arts créatifs"},
        {
            "name": "CULTURAL_CENTRE",
            "label": "Centre culturel pluridisciplinaire",
        },
        {"name": "CULTURAL_MEDIATION", "label": "Médiation culturelle"},
        {
            "name": "DISTRIBUTION_STORE",
            "label": "Magasin de distribution de produits culturels",
        },
        {"name": "FESTIVAL", "label": "Festival"},
        {
            "name": "HERITAGE_SITE",
            "label": "Site patrimonial, historique ou touristique",
        },
        {"name": "LIBRARY", "label": "Bibliothèque ou médiathèque"},
        {"name": "MUSEUM", "label": "Musée"},
        {
            "name": "MUSIC_INSTRUMENT_STORE",
            "label": "Magasin d’instruments de musique",
        },
        {"name": "PERFORMANCE_HALL", "label": "Salle de spectacles"},
        {"name": "PRESS_OR_MEDIA", "label": "Presse ou média"},
        {
            "name": "PRODUCTION_OR_PROMOTION_COMPANY",
            "label": "Société de production, tourneur ou label",
        },
        {"name": "RECORD_STORE", "label": "Disquaire"},
        {
            "name": "SCIENCE_CENTRE",
            "label": "Centre de culture scientifique, technique et industrielle",
        },
        {
            "name": "STREAMING_PLATFORM",
            "label": "Plateforme de streaming musique ou vidéo",
        },
        {"name": "TOURIST_INFORMATION_CENTRE", "label": "Office de tourisme"},
        {"name": "TRAVELLING_CINEMA", "label": "Cinéma itinérant"},
        {"name": "GAMES_CENTRE", "label": "Espace ludique"},
        {
            "name": "TELEVISION_OR_VIDEO_STREAMING",
            "label": "Télévision ou streaming vidéo",
        },
        {
            "name": "RADIO_OR_MUSIC_STREAMING",
            "label": "Radio ou streaming musical",
        },
        {"name": "OTHER", "label": "Autre"},
    ] %}
    {{ return(venue_activity_label_mapping) }}
{% endmacro %}
