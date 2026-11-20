{% macro get_min_category_counts() %}
    {#
        Thresholds N of the "booked at least N distinct categories" counts of the
        expired cohorts (cumulative: a beneficiary with 3 categories counts for 1, 2, 3).
    #}
    {{ return([1, 2, 3, 4, 5]) }}
{% endmacro %}

{% macro get_spent_share_buckets() %}
    {#
        Buckets of the share of all credits received actually spent at expiration: 0,
        then 10% steps (]0-10], ..., ]90-100]).
        Returns: Array of objects with 'name' (column suffix) and 'max' (inclusive upper
        bound, none for the last bucket). The last bucket has no strict 100% so that
        beneficiaries with a few cents left are counted with those who spent everything.
    #}
    {% set spent_share_buckets = [{"name": "0", "max": 0}] %}
    {% for low in range(0, 90, 10) %}
        {% do spent_share_buckets.append(
            {"name": low ~ "_" ~ (low + 10), "max": (low + 10) / 100}
        ) %}
    {% endfor %}
    {% do spent_share_buckets.append({"name": "90_100", "max": none}) %}
    {{ return(spent_share_buckets) }}
{% endmacro %}
