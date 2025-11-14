{% macro get_dimensions(entity_prefix=none, hierarchy_type="geo") %}
    {#
        Generate standardized dimension definitions for external reporting models.

        Parameters:
            entity_prefix (string): The column prefix for geographic columns. Options:
                - 'venue': venue_region_name, venue_department_name, venue_academy_name
                - 'user': user_region_name, user_department_name
                - 'population': population_region_name, population_department_name
                - 'institution': institution_region_name, institution_academy_name
                - 'partner': partner_region_name, partner_department_name, partner_academy_name
                - none/null: region_name, dep_name, academy_name (bare column names)

            hierarchy_type (string): The geographic hierarchy to use. Options:
                - 'geo': National → Region → Department (NAT/REG/DEP)
                - 'academic': National → Region → Academy (NAT/REG/ACAD)

        Returns:
            Array of dimension objects with 'name' and 'value_expr' keys

        Examples:
            {{ get_dimensions('venue', 'geo') }}
            {{ get_dimensions('user', 'geo') }}
            {{ get_dimensions('institution', 'academic') }}
            {{ get_dimensions(none, 'academic') }}
    #}
    {% if entity_prefix %}
        {% set region_col = entity_prefix ~ "_region_name" %}
        {% set dept_col = entity_prefix ~ "_department_name" %}
        {% set acad_col = entity_prefix ~ "_academy_name" %}
    {% else %}
        {% set region_col = "region_name" %}
        {% set dept_col = "dep_name" %}
        {% set acad_col = "academy_name" %}
    {% endif %}

    {% if hierarchy_type == "geo" %}
        {% set dimensions = [
            {"name": "NAT", "value_expr": "'NAT'"},
            {"name": "REG", "value_expr": region_col},
            {"name": "DEP", "value_expr": dept_col},
        ] %}
    {% elif hierarchy_type == "academic" %}
        {% set dimensions = [
            {"name": "NAT", "value_expr": "'NAT'"},
            {"name": "REG", "value_expr": region_col},
            {"name": "ACAD", "value_expr": acad_col},
        ] %}
    {% else %}
        {{
            exceptions.raise_compiler_error(
                "Invalid hierarchy_type: "
                ~ hierarchy_type
                ~ ". Must be 'geo' or 'academic'."
            )
        }}
    {% endif %}

    {{ return(dimensions) }}
{% endmacro %}
