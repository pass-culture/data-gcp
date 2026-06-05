{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions("partner", "academic_extended") %}

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
        where gcp.first_collective_offer_creation_date is not null
    ),

    historical_max_dates as (
        select
            m.venue_id,
            m.partition_month,
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
            gcp.partner_region_name,
            gcp.partner_academy_name,
            gcp.partner_department_name,
            gcp.partner_epci_code,
            gcp.partner_city_code,
            gcp.partner_type,
            gcp.offerer_id,
            gvt.venue_tag_name,
            date_diff(
                least(current_date(), last_day(bd.partition_month)),
                bd.last_collective_date,
                day
            ) as days_since_last_collective_bookable_date,
            date(ey.educational_year_beginning_date) as educational_year_beginning_date
        from historical_max_dates as bd
        inner join
            {{ ref("mrt_global__cultural_partner") }} as gcp
            on bd.venue_id = gcp.venue_id
        left join
            {{ ref("mrt_global__venue_tag") }} as gvt on gcp.venue_id = gvt.venue_id
        inner join
            {{ ref("mrt_global__offerer") }} as gof on gcp.offerer_id = gof.offerer_id
        left join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on date(bd.partition_month)
            between ey.educational_year_beginning_date
            and ey.educational_year_expiration_date
    ),

    partner_with_template_offers as (
        select
            gcp.partner_id,
            gcp.partner_region_name,
            gcp.partner_academy_name,
            gcp.partner_department_name,
            gcp.partner_city_code,
            gcp.partner_epci_code,
            min(co.collective_offer_creation_date) as first_template_offer_creation_date
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        inner join
            {{ ref("int_global__collective_offer") }} as co
            on gcp.partner_id = co.partner_id
        where co.collective_offer_is_template = true
        group by
            partner_id,
            partner_region_name,
            partner_academy_name,
            partner_department_name,
            partner_city_code,
            partner_epci_code
    ),

    monthly_partner_with_template_offers as (
        select
            partner_region_name,
            partner_academy_name,
            partner_department_name,
            partner_epci_code,
            partner_city_code,
            date_trunc(first_template_offer_creation_date, month) as partition_month,
            count(distinct partner_id) as monthly_new_partners_with_template_offers
        from partner_with_template_offers
        group by
            partition_month,
            partner_region_name,
            partner_academy_name,
            partner_department_name,
            partner_epci_code,
            partner_city_code
    ),

    cumul_partner_template as (
        select
            partition_month,
            partner_region_name,
            partner_academy_name,
            partner_department_name,
            partner_city_code,
            partner_epci_code,
            coalesce(
                sum(monthly_new_partners_with_template_offers) over (
                    partition by
                        partner_region_name,
                        partner_academy_name,
                        partner_department_name,
                        partner_epci_code,
                        partner_city_code
                    order by partition_month asc
                ),
                0
            ) as cumul_partners_with_template_offers
        from monthly_partner_with_template_offers
    )

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'pct_partenaire_culturel_actif_collectif' as kpi_name,
        coalesce(
            count(
                distinct case
                    when days_since_last_collective_bookable_date <= 365 then venue_id
                end
            ),
            0
        ) as numerator,
        coalesce(
            count(
                distinct case
                    when days_since_last_collective_bookable_date >= 0 then venue_id
                end
            ),
            0
        ) as denominator,
        safe_divide(
            coalesce(
                count(
                    distinct case
                        when days_since_last_collective_bookable_date <= 365
                        then venue_id
                    end
                ),
                0
            ),
            coalesce(
                count(
                    distinct case
                        when days_since_last_collective_bookable_date >= 0 then venue_id
                    end
                ),
                0
            )
        ) as kpi
    from partner_details
    where
        1 = 1
        {% if is_incremental() %}
            and partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    union all
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'total_partenaire_avec_offre_vitrine' as kpi_name,
        coalesce(sum(cumul_partners_with_template_offers), 0) as numerator,
        1 as denominator,
        coalesce(sum(cumul_partners_with_template_offers), 0) as kpi
    from cumul_partner_template
    {% if is_incremental() %}
        where
            partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value
{% endfor %}
