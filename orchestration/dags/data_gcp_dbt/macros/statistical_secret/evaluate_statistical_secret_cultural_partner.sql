{% macro evaluate_statistical_secret(
    booking_ref, date_column, amount_column, partner_type_column
) %}

    {% set min_partners_threshold = var("statistical_secret_min_partners", 3) %}
    {% set max_concentration_threshold = var(
        "statistical_secret_max_concentration", 0.85
    ) %}
    {% set rolling_months_count = var("statistical_secret_rolling_months", 6) %}
    {% set rolling_interval_offset = rolling_months_count - 1 %}

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
                booking.booking_is_used is true
                and booking.{{ date_column }} >= '2021-07-01'
            group by
                date_trunc(booking.{{ date_column }}, month),
                booking.venue_department_code,
                booking.venue_epci_code,
                booking.venue_municipality_code,
                booking.venue_id
        ),

        -- Unpivot des mailles géographiques pour traitement unifié
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

        -- Fenêtre glissante sur 6 mois au grain (partition_month, geo_level,
        -- geo_code, venue_id)
        venue_rolling_revenue as (
            select
                months.partition_month,
                unpivoted.geo_level,
                unpivoted.geo_code,
                unpivoted.venue_id,
                sum(unpivoted.total_revenue_amount_venue) as rolling_venue_revenue
            from calendar_months as months
            inner join
                unpivoted_revenue as unpivoted
                on months.partition_month >= unpivoted.booking_month
                and unpivoted.booking_month >= date_sub(
                    months.partition_month, interval {{ rolling_interval_offset }} month
                )
            group by
                months.partition_month,
                unpivoted.geo_level,
                unpivoted.geo_code,
                unpivoted.venue_id
        ),

        -- Calcul des métriques agrégées par maille
        geo_rolling_metrics as (
            select
                partition_month,
                geo_level,
                geo_code,
                sum(rolling_venue_revenue) as total_revenue_amount,
                max(rolling_venue_revenue) as max_venue_revenue_amount,
                safe_divide(
                    max(rolling_venue_revenue), sum(rolling_venue_revenue)
                ) as max_partner_revenue_share,
                count(
                    distinct case when rolling_venue_revenue > 0 then venue_id end
                ) as active_partners_count
            from venue_rolling_revenue
            group by partition_month, geo_level, geo_code
        ),

        -- Évaluation des règles
        geo_secret_evaluation as (
            select
                partition_month,
                geo_level,
                geo_code,
                total_revenue_amount,
                max_venue_revenue_amount,
                max_partner_revenue_share,
                active_partners_count,

                coalesce(
                    active_partners_count <= {{ min_partners_threshold }}, true
                ) as is_secret_by_low_partners,
                coalesce(
                    max_partner_revenue_share > {{ max_concentration_threshold }}, false
                ) as is_secret_by_high_concentration,

                (
                    coalesce(
                        active_partners_count <= {{ min_partners_threshold }}, true
                    )
                    or coalesce(
                        max_partner_revenue_share > {{ max_concentration_threshold }},
                        false
                    )
                ) as is_secret,

                case
                    when
                        coalesce(
                            active_partners_count <= {{ min_partners_threshold }}, true
                        )
                        and coalesce(
                            max_partner_revenue_share
                            > {{ max_concentration_threshold }},
                            false
                        )
                    then 'both'
                    when
                        coalesce(
                            active_partners_count <= {{ min_partners_threshold }}, true
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
            from geo_rolling_metrics
        ),

        -- Référentiel géographique dédoublonné
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

        -- Projection plate sur la grille communale
        final_pivot as (
            select
                months.partition_month,
                geo.department_code,
                geo.department_label,
                geo.epci_code,
                geo.epci_label,
                geo.municipality_code,
                geo.municipality_label,

                -- Métriques Département
                dept.active_partners_count as department_active_partners_count,
                dept.max_partner_revenue_share as department_max_partner_revenue_share,
                dept.total_revenue_amount as department_total_revenue_amount,
                coalesce(dept.is_secret, true) as is_department_secret,
                coalesce(
                    dept.secret_reason_code, 'low_partners'
                ) as department_secret_reason_code,

                -- Métriques EPCI
                epci.active_partners_count as epci_active_partners_count,
                epci.max_partner_revenue_share as epci_max_partner_revenue_share,
                epci.total_revenue_amount as epci_total_revenue_amount,
                coalesce(epci.is_secret, true) as is_epci_secret,
                coalesce(
                    epci.secret_reason_code, 'low_partners'
                ) as epci_secret_reason_code,

                -- Métriques Commune
                muni.active_partners_count as municipality_active_partners_count,
                muni.max_partner_revenue_share
                as municipality_max_partner_revenue_share,
                muni.total_revenue_amount as municipality_total_revenue_amount,
                coalesce(muni.is_secret, true) as is_municipality_secret,
                coalesce(
                    muni.secret_reason_code, 'low_partners'
                ) as municipality_secret_reason_code

            from calendar_months as months
            cross join geo_labels as geo

            left join
                geo_secret_evaluation as dept
                on dept.partition_month = months.partition_month
                and dept.geo_level = 'department'
                and dept.geo_code = geo.department_code

            left join
                geo_secret_evaluation as epci
                on epci.partition_month = months.partition_month
                and epci.geo_level = 'epci'
                and epci.geo_code = geo.epci_code

            left join
                geo_secret_evaluation as muni
                on muni.partition_month = months.partition_month
                and muni.geo_level = 'municipality'
                and muni.geo_code = geo.municipality_code
        )

    select
        date(partition_month) as partition_month,
        department_code,
        department_label,
        epci_code,
        epci_label,
        municipality_code,
        municipality_label,

        -- Métriques
        department_active_partners_count,
        department_max_partner_revenue_share,
        department_total_revenue_amount,
        epci_active_partners_count,
        epci_max_partner_revenue_share,
        epci_total_revenue_amount,
        municipality_active_partners_count,
        municipality_max_partner_revenue_share,
        municipality_total_revenue_amount,

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
            when 'both'
            then 'Les deux raisons (<= 3 partenaires ET > 85% CA)'
            when 'low_partners'
            then 'Nombre de partenaires faible (<= 3)'
            when 'high_concentration'
            then 'Concentration forte du CA (> 85%)'
            else 'Non soumis au secret'
        end as department_secret_reason_label,

        case
            epci_secret_reason_code
            when 'both'
            then 'Les deux raisons (<= 3 partenaires ET > 85% CA)'
            when 'low_partners'
            then 'Nombre de partenaires faible (<= 3)'
            when 'high_concentration'
            then 'Concentration forte du CA (> 85%)'
            else 'Non soumis au secret'
        end as epci_secret_reason_label,

        case
            municipality_secret_reason_code
            when 'both'
            then 'Les deux raisons (<= 3 partenaires ET > 85% CA)'
            when 'low_partners'
            then 'Nombre de partenaires faible (<= 3)'
            when 'high_concentration'
            then 'Concentration forte du CA (> 85%)'
            else 'Non soumis au secret'
        end as municipality_secret_reason_label

    from final_pivot

{% endmacro %}
