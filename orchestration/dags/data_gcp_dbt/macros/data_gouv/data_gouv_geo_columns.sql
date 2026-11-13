{% macro data_gouv_geo_columns() %}
    {#
        Geographic columns of the data.gouv.fr exports, formatted as INSEE COG codes
        so that csv-detective recognises them:
            - region_code is cast to a zero-padded string ('01' for Guadeloupe);
            - the "-1" code of non-localised beneficiaries becomes null, the
              'non localisé' label is kept so that national totals stay complete.
        Overseas collectivities (975, 978, 986, 987, 988) have no region code.
    #}
    if(
        region_code is null or region_code < 0,
        null,
        lpad(cast(cast(region_code as int64) as string), 2, "0")
    ) as region_code,
    region_name,
    nullif(department_code, "-1") as department_code,
    department_name
{% endmacro %}

{% macro data_gouv_month_filter(month_column, first_month) %}
    {#-
        Published months: from first_month (start of the national pass Culture,
        see the models) to the last closed month (the current month is still
        being filled)
    -#}
    {{ month_column }} >= date("{{ first_month }}")
    and {{ month_column }} < date_trunc(date("{{ ds() }}"), month)
{% endmacro %}
