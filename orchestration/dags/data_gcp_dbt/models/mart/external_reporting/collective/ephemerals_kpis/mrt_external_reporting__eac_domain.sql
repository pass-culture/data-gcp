{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions(none, "academic") %}
{% set domains = get_domains() %}

with
    bookings_data as (
        select
            cb.institution_region_name as region_name,
            cb.institution_academy_name as academy_name,
            cod.educational_domain_name as domain_name,
            date_trunc(
                date(cb.collective_booking_creation_date), month
            ) as partition_month,
            coalesce(vt.venue_tag_id is not null, false) as is_labelled_mc,
            coalesce(count(distinct cb.collective_booking_id), 0) as total_bookings,
            coalesce(sum(cb.booking_amount), 0) as total_booking_amount,
            coalesce(sum(cb.collective_stock_number_of_tickets), 0) as total_tickets,
            coalesce(
                count(distinct cb.educational_institution_id), 0
            ) as total_institutions
        from {{ ref("mrt_global__collective_booking") }} as cb
        left join
            {{ ref("mrt_global__collective_offer_domain") }} as cod
            on cb.collective_offer_id = cod.collective_offer_id
        left join
            {{ ref("mrt_global__venue_tag") }} as vt
            on cb.venue_id = vt.venue_id
            and venue_tag_category_label
            = 'Comptage partenaire label et appellation du MC'
        where cb.collective_booking_status in ('CONFIRMED', 'USED', 'REIMBURSED')
        group by partition_month, region_name, academy_name, domain_name, is_labelled_mc
    )

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    {% for domain in domains %}
        {% if not loop.first %}
            union all
        {% endif %}
        select
            partition_month,
            timestamp("{{ ts() }}") as updated_at,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            'total_reservation_{{ domain.name }}' as kpi_name,
            sum(total_bookings) as numerator,
            1 as denominator,
            sum(total_bookings) as kpi
        from bookings_data
        where
            1 = 1
            {% if is_incremental() %}
                and partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            and domain_name = '{{ domain.label }}'
        group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
        union all
        select
            partition_month,
            timestamp("{{ ts() }}") as updated_at,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            'total_montant_depense_{{ domain.name }}' as kpi_name,
            sum(total_booking_amount) as numerator,
            1 as denominator,
            sum(total_booking_amount) as kpi
        from bookings_data
        where
            1 = 1
            {% if is_incremental() %}
                and partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            and domain_name = '{{ domain.label }}'
        group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
        union all
        select
            partition_month,
            timestamp("{{ ts() }}") as updated_at,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            'total_tickets_generes_{{ domain.name }}' as kpi_name,
            sum(total_tickets) as numerator,
            1 as denominator,
            sum(total_tickets) as kpi
        from bookings_data
        where
            1 = 1
            {% if is_incremental() %}
                and partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            and domain_name = '{{ domain.label }}'
        group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
        union all
        select
            partition_month,
            timestamp("{{ ts() }}") as updated_at,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            'total_eple_impliquees_{{ domain.name }}' as kpi_name,
            sum(total_institutions) as numerator,
            1 as denominator,
            sum(total_institutions) as kpi
        from bookings_data
        where
            1 = 1
            {% if is_incremental() %}
                and partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            and domain_name = '{{ domain.label }}'
        group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
        union all
        select
            partition_month,
            timestamp("{{ ts() }}") as updated_at,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            'pct_reservations_labelisees_{{ domain.name }}' as kpi_name,
            sum(case when is_labelled_mc then total_bookings end) as numerator,
            sum(total_bookings) as denominator,
            safe_divide(
                sum(case when is_labelled_mc then total_bookings end),
                sum(total_bookings)
            ) as kpi
        from bookings_data
        where
            1 = 1
            {% if is_incremental() %}
                and partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            and domain_name = '{{ domain.label }}'
        group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    {% endfor %}
{% endfor %}
