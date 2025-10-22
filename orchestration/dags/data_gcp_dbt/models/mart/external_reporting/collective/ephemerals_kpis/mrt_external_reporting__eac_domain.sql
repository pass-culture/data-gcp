{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "region_name"},
    {"name": "ACAD", "value_expr": "academy_name"},
    {"name": "EPCI", "value_expr": "epci_name"},
    {"name": "COM", "value_expr": "city_name"},
] %}

{% set domains = [
    {"name": "architecture", "label": "Architecture"},
    {
        "name": "arts_du_cirque_et_de_la_rue",
        "label": "Arts du cirque et arts de la rue",
    },
    {"name": "arts_numeriques", "label": "Arts numériques"},
    {
        "name": "arts_visuels_plastiques_appliques",
        "label": "Arts visuels, arts plastiques, arts appliqués",
    },
    {"name": "bande_dessinee", "label": "Bande dessinée"},
    {"name": "cinema_audiovisuel", "label": "Cinéma, audiovisuel"},
    {
        "name": "culture_scientifique_technique_industrielle",
        "label": "Culture scientifique, technique et industrielle",
    },
    {"name": "danse", "label": "Danse"},
    {"name": "design", "label": "Design"},
    {"name": "developpement_durable", "label": "Développement durable"},
    {"name": "musique", "label": "Musique"},
    {"name": "media_et_information", "label": "Média et information"},
    {"name": "memoire", "label": "Mémoire"},
    {"name": "patrimoine", "label": "Patrimoine"},
    {"name": "photographie", "label": "Photographie"},
    {
        "name": "theatre",
        "label": "Théâtre, expression dramatique, marionnettes",
    },
    {
        "name": "livre_lecture_ecriture",
        "label": "Univers du livre, de la lecture et des écritures",
    },
] %}

with
    bookings_data as (
        select
            cb.institution_region_name as region_name,
            cb.institution_academy_name as academy_name,
            cb.institution_epci as epci_name,
            cb.institution_city as city_name,
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
        group by
            partition_month,
            region_name,
            academy_name,
            epci_name,
            city_name,
            domain_name,
            is_labelled_mc
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
