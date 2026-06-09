{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

-- depends_on: {{ ref('mrt_global__cultural_partner') }}
{% set dimensions_geo = get_dimensions("partner", "geo_epci") %}
{% set dimensions_granular_only = get_dimensions("partner", "granular_only") %}
{% set partner_types = get_partner_types() %}

with
    monthly_partner_activity as (
        select
            gcp.venue_id,
            date_trunc(
                date_add(date('2022-01-01'), interval offset month), month
            ) as partition_month
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        cross join
            unnest(generate_array(0, date_diff(current_date(), '2022-01-01', month))) as
        offset
        where
            gcp.first_individual_offer_creation_date is not null
            or gcp.first_collective_offer_creation_date is not null
    ),

    historical_max_dates as (
        select
            m.venue_id,
            m.partition_month,
            max(
                case
                    when h.total_individual_bookable_offers > 0 then h.partition_date
                end
            ) as last_indiv_date,
            max(
                case
                    when h.total_collective_bookable_offers > 0 then h.partition_date
                end
            ) as last_collective_date
        from monthly_partner_activity as m
        left join
            {{ ref("int_history__bookable_venue") }} as h
            on m.venue_id = h.venue_id
            and h.partition_date <= last_day(m.partition_month)
        group by m.venue_id, m.partition_month
    ),

    partner_details as (
        select
            bd.venue_id,
            bd.partition_month,
            date_diff(
                least(current_date(), last_day(bd.partition_month)),
                bd.last_indiv_date,
                day
            ) as days_since_last_indiv_bookable_date,
            date_diff(
                least(current_date(), last_day(bd.partition_month)),
                bd.last_collective_date,
                day
            ) as days_since_last_collective_bookable_date,
            gcp.partner_region_name,
            gcp.partner_department_name,
            gcp.partner_epci_code,
            gcp.partner_city_code,
            gcp.partner_type,
            gcp.offerer_id,
            gvt.venue_tag_name
        from historical_max_dates as bd
        inner join
            {{ ref("mrt_global__cultural_partner") }} as gcp
            on bd.venue_id = gcp.venue_id
        left join
            {{ ref("mrt_global__venue_tag") }} as gvt
            on gcp.venue_id = gvt.venue_id
            and gvt.venue_tag_category_label
            = "Comptage partenaire label et appellation du MC"
        inner join
            {{ ref("mrt_global__offerer") }} as gof on gcp.offerer_id = gof.offerer_id
    ),

    -- Calcul des entités EPN enlevées, en attente d'alignement sur la manière de les
    -- comptabiliser géographiquement
    -- ,epn_details as (
    -- select
    -- offerer.offerer_region_name as partner_region_name,
    -- offerer.offerer_department_name as partner_department_name,
    -- date_trunc(date(offerer.offerer_creation_date), month) as partition_month,
    -- count(distinct offerer.offerer_id) as epn_created
    -- from {{ ref("mrt_global__offerer") }} as offerer
    -- where offerer_is_epn
    -- group by partition_month, partner_region_name, partner_department_name
    -- ),
    -- Générer la série complète de mois et régions/départements
    -- date_range as (
    -- select
    -- date_trunc(
    -- date_add(
    -- (select min(partition_month) from epn_details),
    -- interval generate_month month
    -- ),
    -- month
    -- ) as partition_month
    -- from
    -- unnest(
    -- generate_array(
    -- 0,
    -- date_diff(
    -- date_trunc(
    -- date_sub(date("{{ ds() }}"), interval 1 month), month
    -- ),
    -- (select min(partition_month) from epn_details),
    -- month
    -- )
    -- )
    -- ) as generate_month
    -- ),
    -- regions_departments as (
    -- select distinct partner_region_name, partner_department_name from epn_details
    -- ),
    -- complete_grid as (
    -- select dr.partition_month, rd.partner_region_name, rd.partner_department_name
    -- from date_range as dr
    -- cross join regions_departments as rd
    -- ),
    -- epn_with_zeros as (
    -- select
    -- cg.partition_month,
    -- cg.partner_region_name,
    -- cg.partner_department_name,
    -- coalesce(ed.epn_created, 0) as epn_created
    -- from complete_grid as cg
    -- left join
    -- epn_details as ed
    -- on cg.partition_month = ed.partition_month
    -- and cg.partner_region_name = ed.partner_region_name
    -- and cg.partner_department_name = ed.partner_department_name
    -- ),
    -- -- Calculer le cumul
    -- cumul_epn_details as (
    -- select
    -- partition_month,
    -- partner_region_name,
    -- partner_department_name,
    -- epn_created,
    -- sum(epn_created) over (
    -- partition by partner_region_name, partner_department_name
    -- order by partition_month asc
    -- rows unbounded preceding
    -- ) as cumul_epn_created
    -- from epn_with_zeros
    -- )
    -- KPIs pour dimensions géographiques (NAT/REG/DEP) incluant les EPN
    final_output as (
        {% for dim in dimensions_geo %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'nombre_total_de_partenaire_actif_individuel' as kpi_name,
                coalesce(
                    count(
                        distinct case
                            when days_since_last_indiv_bookable_date <= 365
                            then venue_id
                        end
                    ),
                    0
                ) as numerator,
                1 as denominator,
                coalesce(
                    count(
                        distinct case
                            when days_since_last_indiv_bookable_date <= 365
                            then venue_id
                        end
                    ),
                    0
                ) as kpi
            from partner_details
            where
                1 = 1
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
            union all
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'nombre_total_de_partenaire_actif_collectif' as kpi_name,
                coalesce(
                    count(
                        distinct case
                            when days_since_last_collective_bookable_date <= 365
                            then venue_id
                        end
                    ),
                    0
                ) as numerator,
                1 as denominator,
                coalesce(
                    count(
                        distinct case
                            when days_since_last_collective_bookable_date <= 365
                            then venue_id
                        end
                    ),
                    0
                ) as kpi
            from partner_details
            where
                1 = 1
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
            union all
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'nombre_total_de_partenaire_actif_deux_volets' as kpi_name,
                coalesce(
                    count(
                        distinct case
                            when
                                (
                                    days_since_last_indiv_bookable_date <= 365
                                    and days_since_last_collective_bookable_date <= 365
                                )
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
                                (
                                    days_since_last_indiv_bookable_date <= 365
                                    and days_since_last_collective_bookable_date <= 365
                                )
                            then venue_id
                        end
                    ),
                    0
                ) as kpi
            from partner_details
            where
                1 = 1
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
            union all
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'nombre_total_de_partenaire_actif_individuel_ou_collectif' as kpi_name,
                coalesce(
                    count(
                        distinct case
                            when
                                (
                                    days_since_last_indiv_bookable_date <= 365
                                    or days_since_last_collective_bookable_date <= 365
                                )
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
                                (
                                    days_since_last_indiv_bookable_date <= 365
                                    or days_since_last_collective_bookable_date <= 365
                                )
                            then venue_id
                        end
                    ),
                    0
                ) as kpi
            from partner_details
            where
                1 = 1
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
            union all
            {% for partner_type in partner_types %}
                {% if not loop.first %}
                    union all
                {% endif %}
                select
                    partition_month,
                    timestamp("{{ ts() }}") as updated_at,
                    '{{ dim.name }}' as dimension_name,
                    {{ dim.value_expr }} as dimension_value,
                    "nombre_de_partenaire_actif_individuel_{{ partner_type.name }}"
                    as kpi_name,
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
                        and partition_month = date_trunc(
                            date_sub(date("{{ ds() }}"), interval 1 month), month
                        )
                    {% endif %}
                group by
                    partition_month,
                    updated_at,
                    dimension_name,
                    dimension_value,
                    kpi_name
            {% endfor %}
            union all
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'nombre_total_cumule_de_partenaires_actives_individuel' as kpi_name,
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
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
            union all
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'nombre_total_cumule_de_partenaires_actives_collectif' as kpi_name,
                coalesce(
                    count(
                        distinct case
                            when days_since_last_collective_bookable_date >= 0
                            then venue_id
                        end
                    ),
                    0
                ) as numerator,
                1 as denominator,
                coalesce(
                    count(
                        distinct case
                            when days_since_last_collective_bookable_date >= 0
                            then venue_id
                        end
                    ),
                    0
                ) as kpi
            from partner_details
            where
                1 = 1
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
            union all
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'nombre_total_cumule_de_partenaires_actives_deux_volets' as kpi_name,
                coalesce(
                    count(
                        distinct case
                            when
                                (
                                    days_since_last_collective_bookable_date >= 0
                                    and days_since_last_indiv_bookable_date >= 0
                                )
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
                                (
                                    days_since_last_collective_bookable_date >= 0
                                    and days_since_last_indiv_bookable_date >= 0
                                )
                            then venue_id
                        end
                    ),
                    0
                ) as kpi
            from partner_details
            where
                1 = 1
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
            union all
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'nombre_total_cumule_de_partenaires_actives_indiv_sans_collectif'
                as kpi_name,
                coalesce(
                    count(
                        distinct case
                            when
                                days_since_last_indiv_bookable_date is not null
                                and days_since_last_collective_bookable_date is null
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
                                days_since_last_indiv_bookable_date is not null
                                and days_since_last_collective_bookable_date is null
                            then venue_id
                        end
                    ),
                    0
                ) as kpi
            from partner_details
            where
                1 = 1
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
            union all
            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'nombre_total_cumule_de_partenaires_actives_collectif_sans_indiv'
                as kpi_name,
                coalesce(
                    count(
                        distinct case
                            when
                                days_since_last_collective_bookable_date is not null
                                and days_since_last_indiv_bookable_date is null
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
                                days_since_last_collective_bookable_date is not null
                                and days_since_last_indiv_bookable_date is null
                            then venue_id
                        end
                    ),
                    0
                ) as kpi
            from partner_details
            where
                1 = 1
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        -- union all
        -- select
        -- epn.partition_month,
        -- timestamp("{{ ts() }}") as updated_at,
        -- '{{ dim.name }}' as dimension_name,
        -- {{ dim.value_expr }} as dimension_value,
        -- 'total_entite_epn' as kpi_name,
        -- coalesce(sum(epn.cumul_epn_created), 0) as numerator,
        -- 1 as denominator,
        -- coalesce(sum(epn.cumul_epn_created), 0) as kpi
        -- from cumul_epn_details as epn
        -- where
        -- 1 = 1
        -- {% if is_incremental() %}
        -- and epn.partition_month
        -- = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        -- {% endif %}
        -- group by partition_month, updated_at, dimension_name, dimension_value,
        -- kpi_name
        {% endfor %}
    )

select *
from final_output
where dimension_value is not null
