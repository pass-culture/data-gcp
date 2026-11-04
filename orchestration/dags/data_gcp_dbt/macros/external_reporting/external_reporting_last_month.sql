{% macro external_reporting_last_month() -%}
    {#
        Last consolidated month of external reporting (SQL date expression).

        Month M is consolidated on the 15th of M + 1: until the 14th the last
        consolidated month is M - 1. E.g. on 2026-11-03 it is 2026-09-01, on
        2026-11-15 it is 2026-10-01, on 2027-01-03 it is 2026-11-01.
    #}
    date_sub(
        date_trunc(date_sub(date('{{ ds() }}'), interval 14 day), month),
        interval 1 month
    )
{%- endmacro %}
