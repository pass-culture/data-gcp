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
                    ) as is_secret_primary,

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
                    end as primary_secret_reason_code
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
                    coalesce(
                        dept.is_secret_primary, true
                    ) as is_department_secret_primary,
                    coalesce(
                        dept.primary_secret_reason_code, 'no_revenue'
                    ) as department_secret_reason_code_primary,
                    (
                        coalesce(dept.total_active_partners_count, 0) > 0
                        or coalesce(dept.total_revenue_amount, 0) > 0
                    ) as has_department_activity,

                    -- EPCI
                    epci.total_active_partners_count
                    as total_active_partners_count_epci,
                    epci.max_partner_revenue_share as max_partner_revenue_share_epci,
                    coalesce(epci.total_revenue_amount, 0) as total_revenue_amount_epci,
                    coalesce(epci.is_secret_primary, true) as is_epci_secret_primary,
                    coalesce(
                        epci.primary_secret_reason_code, 'no_revenue'
                    ) as epci_secret_reason_code_primary,
                    (
                        coalesce(epci.total_active_partners_count, 0) > 0
                        or coalesce(epci.total_revenue_amount, 0) > 0
                    ) as has_epci_activity,

                    -- Commune
                    muni.total_active_partners_count
                    as total_active_partners_count_municipality,
                    muni.max_partner_revenue_share
                    as max_partner_revenue_share_municipality,
                    coalesce(
                        muni.total_revenue_amount, 0
                    ) as total_revenue_amount_municipality,
                    coalesce(
                        muni.is_secret_primary, true
                    ) as is_municipality_secret_primary,
                    coalesce(
                        muni.primary_secret_reason_code, 'no_revenue'
                    ) as municipality_secret_reason_code_primary,
                    (
                        coalesce(muni.total_active_partners_count, 0) > 0
                        or coalesce(muni.total_revenue_amount, 0) > 0
                    ) as has_municipality_activity

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

            -- -----------------------------------------------------------------
            -- SECRET SECONDAIRE : RÈGLE DU 2ÈME PLUS PETIT DÉDOUBLONNÉE
            -- -----------------------------------------------------------------
            secondary_secret_evaluation_{{ months_count }}m as (
                select
                    pivot.*,

                    -- 1. Secret secondaire Commune (au sein de l'EPCI parent)
                    count(
                        distinct case
                            when
                                pivot.is_municipality_secret_primary is true
                                and pivot.has_municipality_activity is true
                            then pivot.municipality_code
                        end
                    ) over (partition by pivot.partition_month, pivot.epci_code)
                    as primary_secret_municipalities_in_epci_count,

                    dense_rank() over (
                        partition by
                            pivot.partition_month,
                            pivot.epci_code,
                            pivot.has_municipality_activity,
                            pivot.is_municipality_secret_primary
                        order by
                            pivot.total_revenue_amount_municipality asc,
                            pivot.municipality_code asc
                    ) as municipality_revenue_rank_in_epci,

                    -- 2. Secret secondaire EPCI (au sein du Département parent)
                    count(
                        distinct case
                            when
                                pivot.is_epci_secret_primary is true
                                and pivot.has_epci_activity is true
                            then pivot.epci_code
                        end
                    ) over (partition by pivot.partition_month, pivot.department_code)
                    as primary_secret_epcis_in_department_count,

                    dense_rank() over (
                        partition by
                            pivot.partition_month,
                            pivot.department_code,
                            pivot.has_epci_activity,
                            pivot.is_epci_secret_primary
                        order by
                            pivot.total_revenue_amount_epci asc, pivot.epci_code asc
                    ) as epci_revenue_rank_in_department
                from final_pivot_{{ months_count }}m as pivot
            ),

            -- -----------------------------------------------------------------
            -- SECRET SECONDAIRE DÉPARTEMENT : RÉGION PUIS GROUPE DE TERRITOIRES
            -- -----------------------------------------------------------------
            department_base_{{ months_count }}m as (
                select
                    dept.partition_month,
                    dept.geo_code as department_code,
                    -- departments missing from the referential (e.g. a venue
                    -- geocoded abroad) form their own region and group, so that
                    -- they never mask a real department
                    coalesce(rd.region_name, 'non localisé') as region_name,
                    if(
                        rd.num_dep is null,
                        'NON_LOCALISE',
                        {{ territory_group("dept.geo_code") }}
                    ) as territory_group,
                    coalesce(dept.total_revenue_amount, 0) as revenue,
                    coalesce(dept.total_active_partners_count, 0) as partners,
                    coalesce(dept.max_revenue_amount_venue, 0) as max_venue_revenue,
                    dept.total_active_partners_count,
                    dept.max_partner_revenue_share,
                    dept.is_secret_primary,
                    dept.primary_secret_reason_code,
                    dept.is_secret_primary
                    and (
                        coalesce(dept.total_active_partners_count, 0) > 0
                        or coalesce(dept.total_revenue_amount, 0) > 0
                    ) as is_masked_primary
                from geo_secret_evaluation_{{ months_count }}m as dept
                left join
                    {{ source("seed", "region_department") }} as rd
                    on dept.geo_code = rd.num_dep
                where dept.geo_level = 'department'
            ),

            {{
                secret_residual_pass(
                    "department_region_" ~ months_count ~ "m",
                    "department_base_" ~ months_count ~ "m",
                    "region_name",
                    "is_masked_primary",
                    "is_masked_region",
                    min_partners_threshold,
                    max_concentration_threshold,
                )
            }},

            {{
                secret_residual_pass(
                    "department_group_" ~ months_count ~ "m",
                    "department_region_" ~ months_count ~ "m",
                    "territory_group",
                    "is_masked_region",
                    "is_masked_group",
                    min_partners_threshold,
                    max_concentration_threshold,
                )
            }},

            -- a region (or territory group) total is secret when what is masked in
            -- it, subtracted from the total, does not pass the primary rules. A
            -- region total is also secret when it holds a department masked at the
            -- territory group step: published, it would give that department by
            -- subtraction and split the masked set of the group
            {% for parent in ["region_name", "territory_group"] %}
                {{ parent }}_secret_{{ months_count }}m as (
                    select
                        partition_month,
                        {{ parent }},
                        {% if parent == "region_name" %}
                            logical_or(is_masked_group and not is_masked_region) or
                        {% endif %}
                        logical_or(is_masked_group)
                        and not coalesce(
                            {{
                                passes_primary_secret(
                                    "sum(if(is_masked_group, partners, 0))",
                                    "sum(if(is_masked_group, revenue, 0))",
                                    "max(if(is_masked_group, max_venue_revenue, 0))",
                                    min_partners_threshold,
                                    max_concentration_threshold,
                                )
                            }},
                            false
                        ) as is_secret
                    from department_group_{{ months_count }}m
                    group by partition_month, {{ parent }}
                ),
            {% endfor %}

            department_secret_{{ months_count }}m as (
                select
                    dept.partition_month,
                    dept.department_code,
                    dept.region_name,
                    dept.territory_group,
                    dept.is_masked_group as is_department_masked,
                    dept.total_active_partners_count,
                    dept.max_partner_revenue_share,
                    dept.revenue,
                    dept.is_secret_primary,
                    dept.primary_secret_reason_code,
                    reg.is_secret as is_region_secret,
                    grp.is_secret as is_territory_group_secret
                from department_group_{{ months_count }}m as dept
                left join
                    region_name_secret_{{ months_count }}m as reg
                    on dept.partition_month = reg.partition_month
                    and dept.region_name = reg.region_name
                left join
                    territory_group_secret_{{ months_count }}m as grp
                    on dept.partition_month = grp.partition_month
                    and dept.territory_group = grp.territory_group
            ),
        {% endfor %}

        combined_windows as (
            {% for months_count in rolling_months_list %}
                -- departments missing from the referential (territory group
                -- NON_LOCALISE) have no municipality row: the full join keeps one
                -- row per such department, published as 'non localisé'
                select
                    coalesce(
                        eval.partition_month, dept_secret.partition_month
                    ) as partition_month,
                    cast({{ months_count }} as int64) as rolling_window_months_count,
                    coalesce(
                        eval.department_code, dept_secret.department_code
                    ) as department_code,
                    if(
                        eval.partition_month is null,
                        'non localisé',
                        eval.department_label
                    ) as department_label,
                    eval.epci_code,
                    eval.epci_label,
                    eval.municipality_code,
                    eval.municipality_label,

                    -- Métriques
                    if(
                        eval.partition_month is null,
                        dept_secret.total_active_partners_count,
                        eval.total_active_partners_count_department
                    ) as total_active_partners_count_department,
                    if(
                        eval.partition_month is null,
                        dept_secret.max_partner_revenue_share,
                        eval.max_partner_revenue_share_department
                    ) as max_partner_revenue_share_department,
                    if(
                        eval.partition_month is null,
                        dept_secret.revenue,
                        eval.total_revenue_amount_department
                    ) as total_revenue_amount_department,
                    eval.total_active_partners_count_epci,
                    eval.max_partner_revenue_share_epci,
                    coalesce(
                        eval.total_revenue_amount_epci, 0
                    ) as total_revenue_amount_epci,
                    eval.total_active_partners_count_municipality,
                    eval.max_partner_revenue_share_municipality,
                    coalesce(
                        eval.total_revenue_amount_municipality, 0
                    ) as total_revenue_amount_municipality,
                    coalesce(
                        eval.has_department_activity, true
                    ) as has_department_activity,
                    coalesce(eval.has_epci_activity, false) as has_epci_activity,
                    coalesce(
                        eval.has_municipality_activity, false
                    ) as has_municipality_activity,

                    -- Évaluation Département (Secret secondaire dans la région, puis
                    -- dans le groupe de territoires)
                    dept_secret.region_name,
                    coalesce(
                        dept_secret.territory_group,
                        {{ territory_group("eval.department_code") }}
                    ) as territory_group,
                    coalesce(
                        if(
                            eval.partition_month is null,
                            dept_secret.is_secret_primary,
                            eval.is_department_secret_primary
                        ),
                        false
                    )
                    or coalesce(
                        dept_secret.is_department_masked, false
                    ) as is_department_secret,
                    coalesce(dept_secret.is_region_secret, false) as is_region_secret,
                    coalesce(
                        dept_secret.is_territory_group_secret, false
                    ) as is_territory_group_secret,

                    case
                        when
                            eval.partition_month is null
                            and dept_secret.is_secret_primary
                        then dept_secret.primary_secret_reason_code
                        when eval.is_department_secret_primary is true
                        then eval.department_secret_reason_code_primary
                        when coalesce(dept_secret.is_department_masked, false)
                        then 'secondary_secret'
                        else 'none'
                    end as department_secret_reason_code,

                    -- Évaluation EPCI (Secret secondaire au niveau Départemental)
                    coalesce(
                        eval.is_epci_secret_primary is true
                        or (
                            eval.primary_secret_epcis_in_department_count = 1
                            and eval.epci_revenue_rank_in_department = 1
                            and eval.has_epci_activity is true
                        ),
                        false
                    ) as is_epci_secret,

                    case
                        when eval.is_epci_secret_primary is true
                        then eval.epci_secret_reason_code_primary
                        when
                            eval.primary_secret_epcis_in_department_count = 1
                            and eval.epci_revenue_rank_in_department = 1
                            and eval.has_epci_activity is true
                        then 'secondary_secret'
                        else 'none'
                    end as epci_secret_reason_code,

                    -- Évaluation Commune (Secret secondaire au niveau EPCI)
                    coalesce(
                        eval.is_municipality_secret_primary is true
                        or (
                            eval.primary_secret_municipalities_in_epci_count = 1
                            and eval.municipality_revenue_rank_in_epci = 1
                            and eval.has_municipality_activity is true
                        ),
                        false
                    ) as is_municipality_secret,

                    case
                        when eval.is_municipality_secret_primary is true
                        then eval.municipality_secret_reason_code_primary
                        when
                            eval.primary_secret_municipalities_in_epci_count = 1
                            and eval.municipality_revenue_rank_in_epci = 1
                            and eval.has_municipality_activity is true
                        then 'secondary_secret'
                        else 'none'
                    end as municipality_secret_reason_code

                from secondary_secret_evaluation_{{ months_count }}m as eval
                full outer join
                    department_secret_{{ months_count }}m as dept_secret
                    on eval.partition_month = dept_secret.partition_month
                    and eval.department_code = dept_secret.department_code
                where
                    eval.partition_month is not null
                    or (
                        dept_secret.territory_group = 'NON_LOCALISE'
                        and (
                            coalesce(dept_secret.total_active_partners_count, 0) > 0
                            or coalesce(dept_secret.revenue, 0) > 0
                        )
                    )
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
        region_name,
        territory_group,

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
        is_region_secret as is_region_secret_stat,
        is_territory_group_secret as is_territory_group_secret_stat,
        is_department_secret,
        is_epci_secret,
        is_municipality_secret,
        has_department_activity,
        has_epci_activity,
        has_municipality_activity,

        -- Masquage à appliquer : secret uniquement si le territoire a de l'activité
        has_department_activity and is_department_secret as is_department_secret_stat,
        has_epci_activity and is_epci_secret as is_epci_secret_stat,
        has_municipality_activity
        and is_municipality_secret as is_municipality_secret_stat,

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
            when 'secondary_secret'
            then 'Secret secondaire (masquage par soustraction)'
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
            when 'secondary_secret'
            then 'Secret secondaire (masquage par soustraction)'
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
            when 'secondary_secret'
            then 'Secret secondaire (masquage par soustraction)'
            else 'Non soumis au secret'
        end as municipality_secret_reason_label

    from combined_windows

{% endmacro %}
