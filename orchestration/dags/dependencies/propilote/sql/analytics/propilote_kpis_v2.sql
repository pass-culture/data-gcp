{% for kpi_details in params.kpis_list %}
    {% for granularity in  ['REG', 'DEPT', 'NAT'] %}
        SELECT
            cast(mois as date) as mois
            , cast(dimension_name as STRING) as dimension_name			
            , cast(dimension_value as STRING) as dimension_value
            , cast(user_type as STRING) as user_type
            , cast(indicator as STRING) as indicator
            , cast(numerator as INTEGER) as numerator
            , cast(denominator as INTEGER) as denominator
        FROM
            `{{ bigquery_tmp_dataset }}.{{ kpi_details.table_name }}_{{ granularity }}`
    {% if not loop.last %}
        UNION ALL 
    {% endif %}
    {% endfor %}

{% if not loop.last %}
    UNION ALL 
{% endif %} 
{% endfor %}