{% macro scholar_year_partition_month(date_column, educational_year_alias) %}
    {#
        Month of date_column, clamped to the bounds of its scholar year so that
        summing Sep-Aug partitions gives the scholar year total (e.g. a booking
        created in June for the next scholar year lands in September).
        Falls back to the raw month when the scholar year is unknown.
        Args:
            date_column: date/timestamp column to bucket by month
            educational_year_alias: alias of the joined
                applicative_database_educational_year source
    #}
    coalesce(
        least(
            greatest(
                date_trunc(date({{ date_column }}), month),
                date({{ educational_year_alias }}.educational_year_beginning_date)
            ),
            date_trunc(
                date({{ educational_year_alias }}.educational_year_expiration_date),
                month
            )
        ),
        date_trunc(date({{ date_column }}), month)
    )
{% endmacro %}
