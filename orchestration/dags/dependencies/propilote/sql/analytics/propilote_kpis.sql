{% for kpi_details in params.kpis_list %}
    {% for granularity in  ['region', 'departement', 'academie','all'] %}
        SELECT
            cast(month as date) as month
            , True AS is_current_year
            , cast(dimension_name as STRING) as dimension_name			
            , cast(dimension_value as STRING) as dimension_value
            , cast(user_type as STRING) as user_type
            , cast(indicator as STRING) as indicator
            , cast(numerator as INTEGER) as numerator
            , cast(denominator as INTEGER) as denominator
        FROM
            `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_{{ kpi_details.table_name }}_{{ granularity }}`
    
        UNION ALL 

        SELECT
            
            date(DATE_TRUNC(DATE_ADD(cast(month as date), interval 1 year), MONTH)) as month
            , False AS is_current_year
            , cast(dimension_name as STRING) as dimension_name			
            , cast(dimension_value as STRING) as dimension_value
            , cast(user_type as STRING) as user_type
            , cast(indicator as STRING) as indicator
            , cast(numerator as INTEGER) as numerator
            , cast(denominator as INTEGER) as denominator
        FROM
            `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_{{ kpi_details.table_name }}_{{ granularity }}`
        WHERE DATE_TRUNC(DATE_SUB(CURRENT_DATE, interval 1 year), MONTH) >= DATE_TRUNC(cast(month as date), MONTH)
    {% if not loop.last %}
        UNION ALL 
    {% endif %}
    
    {% endfor %}

{% if not loop.last %}
    UNION ALL 
{% endif %} 
{% endfor %}