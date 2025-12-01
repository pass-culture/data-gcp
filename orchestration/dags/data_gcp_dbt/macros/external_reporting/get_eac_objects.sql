{% macro get_eac_objects() %}
    {#
        EAC coverage objects (students and institutions).
        Returns: Array of objects with 'name', 'attribute', and 'table_name' keys.
    #}
    {% set objects = [
        {
            "name": "jeunes",
            "attribute": "involved_students",
            "table_name": "adage_involved_student",
        },
        {
            "name": "eple",
            "attribute": "institutions",
            "table_name": "adage_involved_institution",
        },
    ] %}
    {{ return(objects) }}
{% endmacro %}
