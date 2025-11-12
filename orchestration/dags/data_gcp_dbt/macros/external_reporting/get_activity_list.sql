{% macro get_activity_list() %}
    {#
        User activity types for beneficiary segmentation.
        Returns: Array of objects with 'activity' (display name) and 'value' (code) keys.
    #}
    {% set activity_list = [
        {"activity": "Apprenti", "value": "apprenti"},
        {"activity": "Chômeur, En recherche d'emploi", "value": "chomeur"},
        {
            "activity": "Volontaire en service civique rémunéré",
            "value": "volontaire",
        },
        {"activity": "Alternant", "value": "alternant"},
        {"activity": "Employé", "value": "employe"},
        {"activity": "Etudiant", "value": "etudiant"},
        {"activity": "Lycéen", "value": "lyceen"},
        {"activity": "Collégien", "value": "collegien"},
        {
            "activity": "Inactif (ni en emploi ni au chômage), En incapacité de travailler",
            "value": "inactif",
        },
    ] %}
    {{ return(activity_list) }}
{% endmacro %}
