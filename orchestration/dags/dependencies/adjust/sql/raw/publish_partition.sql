select
    {% for column_name in expected_schema %} {{ column_name }}, {% endfor %}
    source_table_name,
    loaded_at
from `{{ table_id }}`
