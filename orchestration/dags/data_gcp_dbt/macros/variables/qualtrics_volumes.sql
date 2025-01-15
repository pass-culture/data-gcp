{% macro qualtrics_volumes() %}
    {% if target.name != "prod" %} {{ return(10) }}
    {% elif this.name == "qualtrics_ac" %} {{ return(3500) }}
    {% elif this.name == "qualtrics_ir_jeunes" %} {{ return(8000) }}
    {% else %} {{ return(0) }}
    {% endif %}
{% endmacro %}
