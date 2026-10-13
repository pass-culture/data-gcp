{% macro evaluate_statistical_secret(
    booking_ref,
    date_column,
    amount_column,
    is_used_column="booking_is_used",
    rolling_months_list=[6, 48],
    min_partners_threshold=3,
    max_concentration_threshold=0.85
) %}

    with
        calendar_months as (
            select distinct date_trunc({{ date_column }}, month) as partition_month
            from {{ booking_ref }}
            where {{ date_column }} >= '2022-01-01'
        ),

        base_venue_revenue as (
            select
                booking.venue_department_code as department_code,
                booking.venue_epci_code as epci_code,
                booking.venue_municipality_code as municipality_code,
                booking.venue_id,
                date_trunc(booking.{{ date_column }}, month) as booking_month,
                sum(booking.{{ amount_column }}) as total_revenue_amount_venue
            from {{ booking_ref }} as booking
            where
                booking.{{ is_used_column }} is true
                and booking.{{ date_column }} >= '2021-07-01'
            group by
                date_trunc(booking.{{ date_column }}, month),
                booking.venue_department_code,
                booking.venue_epci_code,
                booking.venue_municipality_code,
                booking.venue_id
        ),

        unpivoted_revenue as (
            select
                base_rev.booking_month,
                base_rev.venue_id,
                base_rev.total_revenue_amount_venue,
                geo.geo_level,
                geo.geo_code
            from base_venue_revenue as base_rev
            cross join
                unnest(
                    [
                        struct(
                            'department' as geo_level,
                            base_rev.department_code as geo_code
                        ),
                        struct('epci' as geo_level, base_rev.epci_code as geo_code),
                        struct(
                            'municipality' as geo_level,
                            base_rev.municipality_code as geo_code
                        )
                    ]
                ) as geo
            where geo.geo_code is not null
        ),

        geo_labels as (
            select
                geo.municipality_code,
                max(geo.epci_code) as epci_code,
                max(geo.department_code) as department_code,
                max(geo.municipality_label) as municipality_label,
                max(geo.epci_label) as epci_label,
                max(geo.department_name) as department_label
            from {{ ref("int_seed__geo_iris") }} as geo
            where geo.municipality_code is not null
            group by geo.municipality_code
        ),

        {% for months_count in rolling_months_list %}
            {% set offset = months_count - 1 %}

            venue_rolling_revenue_{{ months_count }}m as (
                select
                    months.partition_month,
                    unpivoted.geo_level,
                    unpivoted.geo_code,
                    unpivoted.venue_id,
                    sum(
                        unpivoted.total_revenue_amount_venue
                    ) as total_revenue_amount_venue
                from calendar_months as months
                inner join
                    unpivoted_revenue as unpivoted
                    on months.partition_month >= unpivoted.booking_month
                    and unpivoted.booking_month
                    >= date_sub(months.partition_month, interval {{ offset }} month)
                group by
                    months.partition_month,
                    unpivoted.geo_level,
                    unpivoted.geo_code,
                    unpivoted.venue_id
            ),

            geo_rolling_metrics_{{ months_count }}m as (
                select
                    partition_month,
                    geo_level,
                    geo_code,
                    sum(total_revenue_amount_venue) as total_revenue_amount,
                    max(total_revenue_amount_venue) as max_revenue_amount_venue,
                    safe_divide(
                        max(total_revenue_amount_venue), sum(total_revenue_amount_venue)
                    ) as max_partner_revenue_share,
                    count(
                        distinct case
                            when total_revenue_amount_venue > 0 then venue_id
                        end
                    ) as total_active_partners_count
                from venue_rolling_revenue_{{ months_count }}m
                group by partition_month, geo_level, geo_code
            ),

            geo_secret_evaluation_{{ months_count }}m as (
                select
                    partition_month,
                    geo_level,
                    geo_code,
                    total_revenue_amount,
                    max_revenue_amount_venue,
                    max_partner_revenue_share,
                    total_active_partners_count,

                    coalesce(
                        total_active_partners_count < {{ min_partners_threshold }}, true
                    ) as is_secret_by_low_partners,
                    coalesce(
                        max_partner_revenue_share > {{ max_concentration_threshold }},
                        false
                    ) as is_secret_by_high_concentration,

                    (
                        coalesce(
                            total_active_partners_count < {{ min_partners_threshold }},
                            true
                        )
                        or coalesce(
                            max_partner_revenue_share
                            > {{ max_concentration_threshold }},
                            false
                        )
                    ) as is_secret,

                    case
                        when coalesce(total_revenue_amount, 0) = 0
                        then 'no_revenue'
                        when
                            coalesce(
                                total_active_partners_count
                                < {{ min_partners_threshold }},
                                true
                            )
                            and coalesce(
                                max_partner_revenue_share
                                > {{ max_concentration_threshold }},
                                false
                            )
                        then 'both'
                        when
                            coalesce(
                                total_active_partners_count
                                < {{ min_partners_threshold }},
                                true
                            )
                        then 'low_partners'
                        when
                            coalesce(
                                max_partner_revenue_share
                                > {{ max_concentration_threshold }},
                                false
                            )
                        then 'high_concentration'
                        else 'none'
                    end as secret_reason_code
                from geo_rolling_metrics_{{ months_count }}m
            ),

            final_pivot_{{ months_count }}m as (
                select
                    months.partition_month,
                    cast({{ months_count }} as int64) as rolling_window_months_count,
                    geo.department_code,
                    geo.department_label,
                    geo.epci_code,
                    geo.epci_label,
                    geo.municipality_code,
                    geo.municipality_label,

                    -- Département
                    dept.total_active_partners_count
                    as total_active_partners_count_department,
                    dept.max_partner_revenue_share
                    as max_partner_revenue_share_department,
                    coalesce(
                        dept.total_revenue_amount, 0
                    ) as total_revenue_amount_department,
                    coalesce(dept.is_secret, true) as is_department_secret,
                    coalesce(
                        dept.secret_reason_code, 'no_revenue'
                    ) as department_secret_reason_code,

                    -- EPCI
                    epci.total_active_partners_count
                    as total_active_partners_count_epci,
                    epci.max_partner_revenue_share as max_partner_revenue_share_epci,
                    coalesce(epci.total_revenue_amount, 0) as total_revenue_amount_epci,
                    coalesce(epci.is_secret, true) as is_epci_secret,
                    coalesce(
                        epci.secret_reason_code, 'no_revenue'
                    ) as epci_secret_reason_code,

                    -- Commune
                    muni.total_active_partners_count
                    as total_active_partners_count_municipality,
                    muni.max_partner_revenue_share
                    as max_partner_revenue_share_municipality,
                    coalesce(
                        muni.total_revenue_amount, 0
                    ) as total_revenue_amount_municipality,
                    coalesce(muni.is_secret, true) as is_municipality_secret,
                    coalesce(
                        muni.secret_reason_code, 'no_revenue'
                    ) as municipality_secret_reason_code

                from calendar_months as months
                cross join geo_labels as geo

                left join
                    geo_secret_evaluation_{{ months_count }}m as dept
                    on dept.partition_month = months.partition_month
                    and dept.geo_level = 'department'
                    and dept.geo_code = geo.department_code

                left join
                    geo_secret_evaluation_{{ months_count }}m as epci
                    on epci.partition_month = months.partition_month
                    and epci.geo_level = 'epci'
                    and epci.geo_code = geo.epci_code

                left join
                    geo_secret_evaluation_{{ months_count }}m as muni
                    on muni.partition_month = months.partition_month
                    and muni.geo_level = 'municipality'
                    and muni.geo_code = geo.municipality_code
            ),
        {% endfor %}

        combined_windows as (
            {% for months_count in rolling_months_list %}
                select
                    partition_month,
                    rolling_window_months_count,
                    department_code,
                    department_label,
                    epci_code,
                    epci_label,
                    municipality_code,
                    municipality_label,
                    total_active_partners_count_department,
                    max_partner_revenue_share_department,
                    total_revenue_amount_department,
                    is_department_secret,
                    department_secret_reason_code,
                    total_active_partners_count_epci,
                    max_partner_revenue_share_epci,
                    total_revenue_amount_epci,
                    is_epci_secret,
                    epci_secret_reason_code,
                    total_active_partners_count_municipality,
                    max_partner_revenue_share_municipality,
                    total_revenue_amount_municipality,
                    is_municipality_secret,
                    municipality_secret_reason_code
                from final_pivot_{{ months_count }}m
                {% if not loop.last %}
                    union all
                {% endif %}
            {% endfor %}
        )

    select
        partition_month,
        rolling_window_months_count,
        department_code,
        department_label,
        epci_code,
        epci_label,
        municipality_code,
        municipality_label,

        -- Métriques
        total_active_partners_count_department,
        max_partner_revenue_share_department,
        total_revenue_amount_department,
        total_active_partners_count_epci,
        max_partner_revenue_share_epci,
        total_revenue_amount_epci,
        total_active_partners_count_municipality,
        max_partner_revenue_share_municipality,
        total_revenue_amount_municipality,

        -- Flags
        is_department_secret,
        is_epci_secret,
        is_municipality_secret,

        -- Codes stables
        department_secret_reason_code,
        epci_secret_reason_code,
        municipality_secret_reason_code,

        -- Libellés d'affichage
        case
            department_secret_reason_code
            when 'no_revenue'
            then "Pas de chiffre d'affaire"
            when 'both'
            then 'Les deux raisons (< 3 partenaires ET > 85% CA)'
            when 'low_partners'
            then 'Nombre de partenaires faible (< 3)'
            when 'high_concentration'
            then 'Concentration forte du CA (> 85%)'
            else 'Non soumis au secret'
        end as department_secret_reason_label,

        case
            epci_secret_reason_code
            when 'no_revenue'
            then "Pas de chiffre d'affaire"
            when 'both'
            then 'Les deux raisons (< 3 partenaires ET > 85% CA)'
            when 'low_partners'
            then 'Nombre de partenaires faible (< 3)'
            when 'high_concentration'
            then 'Concentration forte du CA (> 85%)'
            else 'Non soumis au secret'
        end as epci_secret_reason_label,

        case
            municipality_secret_reason_code
            when 'no_revenue'
            then "Pas de chiffre d'affaire"
            when 'both'
            then 'Les deux raisons (< 3 partenaires ET > 85% CA)'
            when 'low_partners'
            then 'Nombre de partenaires faible (< 3)'
            when 'high_concentration'
            then 'Concentration forte du CA (> 85%)'
            else 'Non soumis au secret'
        end as municipality_secret_reason_label

    from combined_windows

{% endmacro %}
