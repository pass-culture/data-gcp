{% for kpi_details in params.kpis_list %}
    {% for granularity in  ['region', 'departement', 'academie','all'] %}
        SELECT
            cast(month as date) as month
            , "TRUE" AS is_current_year
            , cast(dimension_name as STRING) as dimension_name			
            , cast(dimension_value as STRING) as dimension_value
            , cast(user_type as STRING) as user_type
            , cast(indicator as STRING) as indicator
            , cast(numerator as INTEGER) as numerator
            , cast(denominator as INTEGER) as denominator
        FROM
            `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_{{ kpi_details.table_name }}_{{ granularity }}`
    {% if not loop.last %}
        UNION ALL 
    {% endif %}
        SELECT
            -- Changer ici prendre month + 12 month
            cast(month as date) as month
            , "FALSE" AS is_current_year
            , cast(dimension_name as STRING) as dimension_name			
            , cast(dimension_value as STRING) as dimension_value
            , cast(user_type as STRING) as user_type
            , cast(indicator as STRING) as indicator
            , cast(numerator as INTEGER) as numerator
            , cast(denominator as INTEGER) as denominator
        FROM
        -- Changer ici prendre yyyy - 1 
            `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_{{ kpi_details.table_name }}_{{ granularity }}`
    {% if not loop.last %}
        UNION ALL 
    {% endif %}
    
    {% endfor %}

{% if not loop.last %}
    UNION ALL 
{% endif %} 
{% endfor %}