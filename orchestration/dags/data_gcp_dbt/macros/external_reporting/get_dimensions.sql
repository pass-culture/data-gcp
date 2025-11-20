{% macro get_dimensions(
    entity_prefix=none,
    hierarchy_type="geo",
    skip_epn=false
) %}
    {#
        Generate standardized dimension definitions for external reporting models.

        Parameters:
            entity_prefix (string): The column prefix for geographic columns. Options:
                - 'venue': venue_region_name, venue_department_name, venue_academy_name
                - 'user': user_region_name, user_department_name
                - 'population': population_region_name, population_department_name
                - 'institution': institution_region_name, institution_academy_name
                - 'partner': partner_region_name, partner_department_name, partner_academy_name
                - none/null: region_name, dep_name, academy_name, epci_name, city_name (bare column names)

            hierarchy_type (string): The geographic hierarchy to use. Options:
                - 'geo': National → Region → Department (NAT/REG/DEP)
                - 'geo_epci': National → Region → Department → EPCI (NAT/REG/DEP/EPCI)
                - 'geo_full': National → Region → Department → EPCI → COM (NAT/REG/DEP/EPCI/COM)
                - 'academic': National → Region → Academy (NAT/REG/ACAD)
                - 'academic_extended': National → Region → Academy → EPCI → COM (NAT/REG/ACAD/EPCI/COM)

            skip_epn (bool): Add skip_epn flag to dimension objects (for cultural_partner models)

        Returns:
            Array of dimension objects with 'name' and 'value_expr' keys (and optionally 'skip_epn')

        Examples:
            {{ get_dimensions('venue', 'geo') }}
            {{ get_dimensions('user', 'geo_full') }}
            {{ get_dimensions('partner', 'academic_extended') }}
            {{ get_dimensions(none, 'geo_epci') }}
            {{ get_dimensions('partner', 'geo_full', skip_epn=true) }}
            {{ get_dimensions(none, 'academic_extended') }}
    #}
    {% if entity_prefix %}
        {% set region_col = entity_prefix ~ "_region_name" %}
        {% set dept_col = entity_prefix ~ "_department_name" %}
        {% set acad_col = entity_prefix ~ "_academy_name" %}
        {% set epci_col = entity_prefix ~ "_epci" %}
        {% set city_col = entity_prefix ~ "_city" %}
    {% else %}
        {% set region_col = "region_name" %}
        {% set dept_col = "dep_name" %}
        {% set acad_col = "academy_name" %}
        {% set epci_col = "epci_name" %}
        {% set city_col = "city_name" %}
    {% endif %}

    {% if hierarchy_type == "geo" %}
        {% set dimensions = [
            {"name": "NAT", "value_expr": "'NAT'"},
            {"name": "REG", "value_expr": region_col},
            {"name": "DEP", "value_expr": dept_col},
        ] %}
    {% elif hierarchy_type == "geo_epci" %}
        {% set dimensions = [
            {"name": "NAT", "value_expr": "'NAT'"},
            {"name": "REG", "value_expr": region_col},
            {"name": "DEP", "value_expr": dept_col},
            {"name": "EPCI", "value_expr": epci_col},
        ] %}
    {% elif hierarchy_type == "geo_full" %}
        {% set dimensions = [
            {"name": "NAT", "value_expr": "'NAT'"},
            {"name": "REG", "value_expr": region_col},
            {"name": "DEP", "value_expr": dept_col},
            {"name": "EPCI", "value_expr": epci_col},
            {"name": "COM", "value_expr": city_col},
        ] %}
    {% elif hierarchy_type == "academic" %}
        {% set dimensions = [
            {"name": "NAT", "value_expr": "'NAT'"},
            {"name": "REG", "value_expr": region_col},
            {"name": "ACAD", "value_expr": acad_col},
        ] %}
    {% elif hierarchy_type == "academic_extended" %}
        {% set dimensions = [
            {"name": "NAT", "value_expr": "'NAT'"},
            {"name": "REG", "value_expr": region_col},
            {"name": "ACAD", "value_expr": acad_col},
            {"name": "EPCI", "value_expr": epci_col},
            {"name": "COM", "value_expr": city_col},
        ] %}
    {% else %}
        {{
            exceptions.raise_compiler_error(
                "Invalid hierarchy_type: "
                ~ hierarchy_type
                ~ ". Must be one of: 'geo', 'geo_epci', 'geo_full', 'academic', 'academic_extended'."
            )
        }}
    {% endif %}

    {% if skip_epn %}
        {% set dimensions_with_skip = [] %}
        {% for dim in dimensions %}
            {% if dim.name in ["EPCI", "COM"] %}
                {% set _ = dimensions_with_skip.append(
                    {
                        "name": dim.name,
                        "value_expr": dim.value_expr,
                        "skip_epn": true,
                    }
                ) %}
            {% else %}
                {% set _ = dimensions_with_skip.append(
                    {
                        "name": dim.name,
                        "value_expr": dim.value_expr,
                        "skip_epn": false,
                    }
                ) %}
            {% endif %}
        {% endfor %}
        {{ return(dimensions_with_skip) }}
    {% else %} {{ return(dimensions) }}
    {% endif %}
{% endmacro %}
