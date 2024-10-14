{% macro generate_seed_geolocation_query(
    source_table,
    referential_table,
    id_column,
    prefix_name,
    columns,
    geo_shape="geo_shape"
) %}
    select
        {{ id_column }}, {% for column in columns %} ref_data.{{ column }}, {% endfor %}
    from
        {% if source_table | length == 2 %}
            {{ source(source_table[0], source_table[1]) }}
        {% else %} {{ ref(source_table) }}
        {% endif %}
    join
        (
            select
                {% for column in columns %} {{ column }}, {% endfor %}
                {{ geo_shape }} as geo_shape,
                min_longitude,
                min_latitude,
                max_longitude,
                max_latitude
            from
                {% if referential_table | length == 2 %}
                    {{ source(referential_table[0], referential_table[1]) }}
                {% else %} {{ ref(referential_table) }}
                {% endif %}
        ) ref_data
        on {{ prefix_name }}_longitude
        between ref_data.min_longitude and ref_data.max_longitude
        and {{ prefix_name }}_latitude
        between ref_data.min_latitude and ref_data.max_latitude
        and st_contains(
            ref_data.geo_shape,
            st_geogpoint({{ prefix_name }}_longitude, {{ prefix_name }}_latitude)
        )
{% endmacro %}
