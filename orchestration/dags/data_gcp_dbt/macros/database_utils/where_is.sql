{% macro where_is(model_name) %}
    {# Get the node (model) object by name #}
    {% set node = (
        graph.nodes.values()
        | selectattr("name", "equalto", model_name)
        | list
        | first
    ) %}

    {% if not node %}
        {{
            exceptions.raise_compiler_error(
                "Model '" ~ model_name ~ "' not found in project graph."
            )
        }}
    {% endif %}

    {# Resolve schema, alias, and database #}
    {% set database = node.database if node.database else target.database %}
    {% set schema = node.schema if node.schema else target.schema %}
    {% set alias = node.alias if node.alias else node.name %}

    {# Build full relation name #}
    {% set full_name = database ~ "." ~ schema ~ "." ~ alias %}

    {{
        log(
            "Full relation name for model '" ~ model_name ~ "': " ~ full_name,
            info=True,
        )
    }}

    {{ return(full_name) }}
{% endmacro %}
