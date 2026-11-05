{% macro get_actual_beneficiary_breakdowns() %}
    {#
        Profiles of the active beneficiaries counted by int_kpi__beneficiary as
        columns (total_actual_beneficiaries_<name>, cell_key_actual_beneficiaries_<name>)
        rather than as grain columns.
        Returns: Array of objects with 'name' (column suffix) and 'condition' (on the
        user_is_in_education, user_civility and user_activity columns).
    #}
    {% set breakdowns = [
        {"name": "not_in_education", "condition": "not user_is_in_education"},
        {"name": "male", "condition": "user_civility = 'M.'"},
        {"name": "female", "condition": "user_civility = 'Mme.'"},
        {"name": "without_civility", "condition": "user_civility is null"},
    ] %}
    {% for activity in get_activity_list() %}
        {% do breakdowns.append(
            {
                "name": "activity_" ~ activity.value,
                "condition": 'user_activity = "' ~ activity.activity ~ '"',
            }
        ) %}
    {% endfor %}
    {{ return(breakdowns) }}
{% endmacro %}
