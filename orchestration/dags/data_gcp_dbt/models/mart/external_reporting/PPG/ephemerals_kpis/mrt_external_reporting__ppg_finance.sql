{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "execution_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "venue_region_name"},
    {"name": "DEP", "value_expr": "venue_department_name"},
] %}

{% set categories = [
    {"name": "LIVRE", "value_expr": "livres"},
    {"name": "CINEMA", "value_expr": "cinema"},
    {"name": "MUSIQUE_LIVE", "value_expr": "musique_live"},
    {"name": "SPECTACLE", "value_expr": "spectacle_vivant"},
    {"name": "MUSEE", "value_expr": "musee"},
    {"name": "PRATIQUE_ART", "value_expr": "pratiques_artistiques"},
] %}

{% set kpis = [
    {"name": "total_reservations", "value_expr": "total_bookings"},
    {"name": "total_quantites", "value_expr": "total_quantities"},
    {"name": "total_ca", "value_expr": "total_revenue_amount"},
    {
        "name": "total_montant_rembourse",
        "value_expr": "total_reimbursed_amount",
    },
    {
        "name": "total_montant_contribution",
        "value_expr": "total_contribution_amount",
    },
] %}


{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    {% for kpi in kpis %}
        {% if not loop.first %}
            union all
        {% endif %}
        select
            date_trunc(date(booking_used_date), month) as execution_date,
            date("{{ ds() }}") as update_date,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            '{{ kpi.name }}' as kpi_name,
            sum({{ kpi.value_expr }}) as numerator,
            1 as denominator,
            safe_divide(sum({{ kpi.value_expr }}), 1) as kpi
        from {{ ref("mrt_finance__reimbursement") }}
        where
            1 = 1
            {% if is_incremental() %}
                and date_trunc(booking_used_date, month)
                = date_trunc(date("{{ ds() }}"), month)
            {% endif %}
        group by execution_date, update_date, dimension_name, dimension_value, kpi_name
    {% endfor %}
{% endfor %}
{% for category in categories %}
    union all
    {% for dim in dimensions %}
        {% if not loop.first %}
            union all
        {% endif %}
        {% for kpi in kpis %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                date_trunc(date(booking_used_date), month) as execution_date,
                date("{{ ds() }}") as update_date,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                '{{ kpi.name }}_{{ category.value_expr }}' as kpi_name,
                sum({{ kpi.value_expr }}) as numerator,
                1 as denominator,
                safe_divide(sum({{ kpi.value_expr }}), 1) as kpi
            from {{ ref("mrt_finance__reimbursement") }}
            where
                offer_category_id = '{{ category.name }}'
                {% if is_incremental() %}
                    and date_trunc(booking_used_date, month)
                    = date_trunc(date("{{ ds() }}"), month)
                {% endif %}
            group by
                execution_date, update_date, dimension_name, dimension_value, kpi_name
        {% endfor %}
    {% endfor %}
{% endfor %}
