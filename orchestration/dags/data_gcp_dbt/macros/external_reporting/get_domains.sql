{% macro get_domains() %}
    {#
        Educational domains for EAC (Education Artistique et Culturelle) reporting.
        Returns: Array of 17 educational domain objects with 'name' and 'label' keys.
    #}
    {% set domains = [
        {"name": "architecture", "label": "Architecture"},
        {
            "name": "arts_du_cirque_et_de_la_rue",
            "label": "Arts du cirque et arts de la rue",
        },
        {"name": "arts_numeriques", "label": "Arts numériques"},
        {
            "name": "arts_visuels_plastiques_appliques",
            "label": "Arts visuels, arts plastiques, arts appliqués",
        },
        {"name": "bande_dessinee", "label": "Bande dessinée"},
        {"name": "cinema_audiovisuel", "label": "Cinéma, audiovisuel"},
        {
            "name": "culture_scientifique_technique_industrielle",
            "label": "Culture scientifique, technique et industrielle",
        },
        {"name": "danse", "label": "Danse"},
        {"name": "design", "label": "Design"},
        {"name": "developpement_durable", "label": "Développement durable"},
        {"name": "musique", "label": "Musique"},
        {"name": "media_et_information", "label": "Média et information"},
        {"name": "memoire", "label": "Mémoire"},
        {"name": "patrimoine", "label": "Patrimoine"},
        {"name": "photographie", "label": "Photographie"},
        {
            "name": "theatre",
            "label": "Théâtre, expression dramatique, marionnettes",
        },
        {
            "name": "livre_lecture_ecriture",
            "label": "Univers du livre, de la lecture et des écritures",
        },
    ] %}
    {{ return(domains) }}
{% endmacro %}
