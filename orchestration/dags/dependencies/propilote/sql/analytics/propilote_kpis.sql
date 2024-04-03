{% for kpi_details in params.kpis_list %}
    {% for granularity in  ['region', 'departement', 'academie','all'] %}
        SELECT
            safe_cast(month as date) as month
            , True AS is_current_year
            , safe_cast(dimension_name as STRING) as dimension_name			
            , safe_cast(dimension_value as STRING) as dimension_value
            , safe_cast(user_type as STRING) as user_type
            , safe_cast(indicator as STRING) as indicator
            , safe_cast(numerator as INTEGER) as numerator
            , safe_cast(denominator as INTEGER) as denominator
        FROM
            `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_{{ kpi_details.table_name }}_{{ granularity }}`
    
        UNION ALL 

        SELECT
            
            date(DATE_TRUNC(DATE_ADD(safe_cast(month as date), interval 1 year), MONTH)) as month
            , False AS is_current_year
            , safe_cast(dimension_name as STRING) as dimension_name			
            , safe_cast(dimension_value as STRING) as dimension_value
            , safe_cast(user_type as STRING) as user_type
            , safe_cast(indicator as STRING) as indicator
            , safe_cast(numerator as INTEGER) as numerator
            , safe_cast(denominator as INTEGER) as denominator
        FROM
            `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_{{ kpi_details.table_name }}_{{ granularity }}`
        WHERE DATE_TRUNC(DATE_SUB(CURRENT_DATE, interval 1 year), MONTH) >= DATE_TRUNC(safe_cast(month as date), MONTH)
    {% if not loop.last %}
        UNION ALL 
    {% endif %}
    
    {% endfor %}

{% if not loop.last %}
    UNION ALL 
{% endif %} 
{% endfor %}