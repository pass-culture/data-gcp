{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions_geo = get_dimensions("partner", "geo") %}
{% set dimensions_granular = get_dimensions("partner", "geo_full") %}
{% set partner_types = get_partner_types() %}

with
    all_activated_partners_and_days as (
        -- Pour chaque venue_id, une ligne par jour depuis la 1ère offre publiée
        select
            gcp.venue_id,
            gcp.first_individual_offer_creation_date,
            date_add(date('2022-01-01'), interval offset day) as partition_day
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        cross join
            unnest(generate_array(0, date_diff(current_date(), '2022-01-01', day))) as
        offset
        where
            gcp.first_individual_offer_creation_date is not null
            and date_add(date('2022-01-01'), interval offset day)
            >= gcp.first_individual_offer_creation_date
            and date_add(date('2022-01-01'), interval offset day) < current_date()
    ),

    all_days_with_bookability as (
        select
            apd.venue_id,
            apd.first_individual_offer_creation_date,
            apd.partition_day,
            coalesce(
                bvh.total_individual_bookable_offers, 0
            ) as total_indiv_bookable_offers
        from all_activated_partners_and_days as apd
        left join
            {{ ref("int_history__bookable_venue") }} as bvh
            on apd.venue_id = bvh.venue_id
            and apd.partition_day = bvh.partition_date
    ),

    bookable_dates as (
        select
            venue_id,
            first_individual_offer_creation_date,
            partition_day,
            date_diff(
                partition_day,
                coalesce(
                    max(
                        case
                            when total_indiv_bookable_offers != 0 then partition_day
                        end
                    ) over (
                        partition by venue_id
                        order by partition_day
                        rows between unbounded preceding and current row
                    ),
                    first_individual_offer_creation_date
                ),
                day
            ) as days_since_last_indiv_bookable_date
        from all_days_with_bookability
    ),

    partner_details as (
        select
            bd.venue_id,
            bd.partition_day,
            bd.first_individual_offer_creation_date,
            bd.days_since_last_indiv_bookable_date,
            gcp.partner_region_name,
            gcp.partner_department_name,
            gcp.partner_epci,
            gcp.partner_city,
            gcp.partner_type,
            gcp.offerer_id,
            gvt.venue_tag_name
        from bookable_dates as bd
        inner join
            {{ ref("mrt_global__cultural_partner") }} as gcp
            on bd.venue_id = gcp.venue_id
        left join
            {{ ref("mrt_global__venue_tag") }} as gvt on gcp.venue_id = gvt.venue_id
        inner join
            {{ ref("mrt_global__offerer") }} as gof on gcp.offerer_id = gof.offerer_id
    ),

    epn_details as (
        select
            offerer.offerer_region_name as partner_region_name,
            offerer.offerer_department_name as partner_department_name,
            date_trunc(date(offerer.offerer_creation_date), month) as partition_month,
            count(distinct offerer.offerer_id) as epn_created
        from {{ ref("mrt_global__offerer") }} as offerer
        where offerer_is_epn
        group by partition_month, partner_region_name, partner_department_name
    ),

    -- Générer la série complète de mois et régions/départements
    date_range as (
        select
            date_trunc(
                date_add(
                    (select min(partition_month) from epn_details),
                    interval generate_month month
                ),
                month
            ) as partition_month
        from
            unnest(
                generate_array(
                    0,
                    date_diff(
                        date_trunc(
                            date_sub(date("{{ ds() }}"), interval 1 month), month
                        ),
                        (select min(partition_month) from epn_details),
                        month
                    )
                )
            ) as generate_month
    ),

    regions_departments as (
        select distinct partner_region_name, partner_department_name from epn_details
    ),

    complete_grid as (
        select dr.partition_month, rd.partner_region_name, rd.partner_department_name
        from date_range as dr
        cross join regions_departments as rd
    ),

    epn_with_zeros as (
        select
            cg.partition_month,
            cg.partner_region_name,
            cg.partner_department_name,
            coalesce(ed.epn_created, 0) as epn_created
        from complete_grid as cg
        left join
            epn_details as ed
            on cg.partition_month = ed.partition_month
            and cg.partner_region_name = ed.partner_region_name
            and cg.partner_department_name = ed.partner_department_name
    ),

    -- Calculer le cumul
    cumul_epn_details as (
        select
            partition_month,
            partner_region_name,
            partner_department_name,
            epn_created,
            sum(epn_created) over (
                partition by partner_region_name, partner_department_name
                order by partition_month asc
                rows unbounded preceding
            ) as cumul_epn_created
        from epn_with_zeros
    )

-- KPIs pour dimensions géographiques (NAT/REG/DEP) incluant les EPN
{% for dim in dimensions_geo %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(date(partition_day), month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'nombre_total_de_partenaire_actif' as kpi_name,
        coalesce(
            count(
                distinct case
                    when days_since_last_indiv_bookable_date <= 365 then venue_id
                end
            ),
            0
        ) as numerator,
        1 as denominator,
        coalesce(
            count(
                distinct case
                    when days_since_last_indiv_bookable_date <= 365 then venue_id
                end
            ),
            0
        ) as kpi
    from partner_details
    where
        1 = 1
        {% if is_incremental() %}
            and date_trunc(date(partition_day), month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    union all
    {% for partner_type in partner_types %}
        {% if not loop.first %}
            union all
        {% endif %}
        select
            date_trunc(date(partition_day), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            "nombre_de_partenaire_actif_{{ partner_type.name }}" as kpi_name,
            coalesce(
                count(
                    distinct case
                        when
                            days_since_last_indiv_bookable_date <= 365
                            and {{ partner_type.condition }}
                        then venue_id
                    end
                ),
                0
            ) as numerator,
            1 as denominator,
            coalesce(
                count(
                    distinct case
                        when
                            days_since_last_indiv_bookable_date <= 365
                            and {{ partner_type.condition }}
                        then venue_id
                    end
                ),
                0
            ) as kpi
        from partner_details
        where
            1 = 1
            {% if is_incremental() %}
                and date_trunc(date(partition_day), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
        group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    {% endfor %}
    union all
    select
        date_trunc(date(partition_day), month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'nombre_total_cumule_de_partenaire_actif' as kpi_name,
        coalesce(
            count(
                distinct case
                    when days_since_last_indiv_bookable_date >= 0 then venue_id
                end
            ),
            0
        ) as numerator,
        1 as denominator,
        coalesce(
            count(
                distinct case
                    when days_since_last_indiv_bookable_date >= 0 then venue_id
                end
            ),
            0
        ) as kpi
    from partner_details
    where
        1 = 1
        {% if is_incremental() %}
            and date_trunc(date(partition_day), month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    union all
    select
        epn.partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'total_entite_epn' as kpi_name,
        coalesce(sum(epn.cumul_epn_created), 0) as numerator,
        1 as denominator,
        coalesce(sum(epn.cumul_epn_created), 0) as kpi
    from cumul_epn_details as epn
    where
        1 = 1
        {% if is_incremental() %}
            and epn.partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endfor %}

-- KPIs pour dimensions granulaires (NAT/REG/DEP/EPCI/COM) sans les EPN
union all
{% for dim in dimensions_granular %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(date(partition_day), month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'nombre_total_de_partenaire_actif' as kpi_name,
        coalesce(
            count(
                distinct case
                    when days_since_last_indiv_bookable_date <= 365 then venue_id
                end
            ),
            0
        ) as numerator,
        1 as denominator,
        coalesce(
            count(
                distinct case
                    when days_since_last_indiv_bookable_date <= 365 then venue_id
                end
            ),
            0
        ) as kpi
    from partner_details
    where
        1 = 1
        {% if is_incremental() %}
            and date_trunc(date(partition_day), month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    union all
    {% for partner_type in partner_types %}
        {% if not loop.first %}
            union all
        {% endif %}
        select
            date_trunc(date(partition_day), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            "nombre_de_partenaire_actif_{{ partner_type.name }}" as kpi_name,
            coalesce(
                count(
                    distinct case
                        when
                            days_since_last_indiv_bookable_date <= 365
                            and {{ partner_type.condition }}
                        then venue_id
                    end
                ),
                0
            ) as numerator,
            1 as denominator,
            coalesce(
                count(
                    distinct case
                        when
                            days_since_last_indiv_bookable_date <= 365
                            and {{ partner_type.condition }}
                        then venue_id
                    end
                ),
                0
            ) as kpi
        from partner_details
        where
            1 = 1
            {% if is_incremental() %}
                and date_trunc(date(partition_day), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
        group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    {% endfor %}
    union all
    select
        date_trunc(date(partition_day), month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'nombre_total_cumule_de_partenaire_actif' as kpi_name,
        coalesce(
            count(
                distinct case
                    when days_since_last_indiv_bookable_date >= 0 then venue_id
                end
            ),
            0
        ) as numerator,
        1 as denominator,
        coalesce(
            count(
                distinct case
                    when days_since_last_indiv_bookable_date >= 0 then venue_id
                end
            ),
            0
        ) as kpi
    from partner_details
    where
        1 = 1
        {% if is_incremental() %}
            and date_trunc(date(partition_day), month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endfor %}
