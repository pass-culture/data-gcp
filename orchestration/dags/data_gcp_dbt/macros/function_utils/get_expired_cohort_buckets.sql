{% macro get_min_category_counts() %}
    {#
        Thresholds N of the "booked at least N distinct categories" counts of the
        expired cohorts (cumulative: a beneficiary with 3 categories counts for 1, 2, 3).
    #}
    {{ return([1, 2, 3, 4, 5]) }}
{% endmacro %}

{% macro get_spent_share_buckets() %}
    {#
        Buckets of the share of all credits received actually spent at expiration.
        Returns: Array of objects with 'name' (column suffix) and 'max' (inclusive upper
        bound, none for the last bucket). The last bucket has no strict 100% so that
        beneficiaries with a few cents left are counted with those who spent everything.
    #}
    {% set spent_share_buckets = [
        {"name": "0", "max": 0},
        {"name": "0_25", "max": 0.25},
        {"name": "25_50", "max": 0.5},
        {"name": "50_75", "max": 0.75},
        {"name": "75_90", "max": 0.9},
        {"name": "90_100", "max": none},
    ] %}
    {{ return(spent_share_buckets) }}
{% endmacro %}
