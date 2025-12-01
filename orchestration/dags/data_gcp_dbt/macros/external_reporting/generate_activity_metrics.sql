{% macro generate_activity_metrics(base_cte, activity_list, dimensions) %}
    {#
        Generate activity-based metrics for beneficiaries across dimensions.

        Generates percentage metrics for each activity type showing what proportion
        of users have that activity status.

        Parameters:
            base_cte: Name of the CTE containing user_activity and user_id columns
            activity_list: Array of activity objects from get_activity_list()
            dimensions: Array of dimension objects from get_dimensions()

        Returns: SQL with UNION ALL of activity metrics for each activity and dimension
    #}
    {% for activity in activity_list %}
        {% if not loop.first %}
            union all
        {% endif %}
        {{
            generate_metric_by_dimensions(
                base_cte,
                "pct_beneficiaire_actuel_" ~ activity.value,
                'count(distinct case when user_activity = "'
                ~ activity.activity
                ~ '" then user_id end)',
                "count(distinct user_id)",
                dimensions,
            )
        }}
    {% endfor %}
{% endmacro %}
