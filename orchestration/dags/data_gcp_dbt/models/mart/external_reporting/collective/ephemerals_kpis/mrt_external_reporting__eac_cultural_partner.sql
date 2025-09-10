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
    {"name": "REG", "value_expr": "partner_region_name"},
    {"name": "DEP", "value_expr": "partner_department_name"},
] %}

with
    all_activated_partners_and_days as (
        -- Pour chaque partner_id, une ligne par jour depuis la 1ère offre publiée
        select
            gcp.partner_id,
            gcp.first_collective_offer_creation_date,
            date_add(date('2022-01-01'), interval offset day) as partition_day
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        cross join
            unnest(generate_array(0, date_diff(current_date(), '2022-01-01', day))) as
        offset
        where
            gcp.first_collective_offer_creation_date is not null
            and date_add(date('2022-01-01'), interval offset day)
            >= gcp.first_collective_offer_creation_date
            and date_add(date('2022-01-01'), interval offset day) < current_date()
    ),

    all_days_with_bookability as (
        select
            apd.partner_id,
            apd.first_collective_offer_creation_date,
            apd.partition_day,
            coalesce(
                bph.collective_bookable_offers, 0
            ) as total_collective_bookable_offers
        from all_activated_partners_and_days as apd
        left join
            {{ ref("bookable_partner_history") }} as bph
            on apd.partner_id = bph.partner_id
            and apd.partition_day = bph.partition_date
    ),

    bookable_dates as (
        select
            partner_id,
            first_collective_offer_creation_date,
            partition_day,
            date_diff(
                partition_day,
                coalesce(
                    max(
                        case
                            when total_collective_bookable_offers != 0
                            then partition_day
                        end
                    ) over (
                        partition by partner_id
                        order by partition_day
                        rows between unbounded preceding and current row
                    ),
                    first_collective_offer_creation_date
                ),
                day
            ) as days_since_last_collective_bookable_date
        from all_days_with_bookability
    ),

    partner_details as (
        select
            bd.partner_id,
            bd.partition_day,
            bd.first_collective_offer_creation_date,
            bd.days_since_last_collective_bookable_date,
            gcp.partner_region_name,
            gcp.partner_department_name,
            gcp.partner_type,
            gcp.offerer_id,
            gvt.venue_tag_name,
            date(ey.educational_year_beginning_date) as educational_year_beginning_date
        from bookable_dates as bd
        inner join
            {{ ref("mrt_global__cultural_partner") }} as gcp
            on bd.partner_id = gcp.partner_id
        left join
            {{ ref("mrt_global__venue_tag") }} as gvt on gcp.partner_id = gvt.partner_id
        inner join
            {{ ref("mrt_global__offerer") }} as gof on gcp.offerer_id = gof.offerer_id
        left join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on date(bd.partition_day)
            between ey.educational_year_beginning_date
            and ey.educational_year_expiration_date
    ),

    partner_with_template_offers as (
        select
            gcp.partner_id,
            gcp.partner_region_name,
            gcp.partner_department_name,
            min(co.collective_offer_creation_date) as first_template_offer_creation_date
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        inner join
            {{ ref("int_global__collective_offer") }} as co
            on gcp.partner_id = co.partner_id
        where co.collective_offer_is_template = true
        group by partner_id, partner_region_name, partner_department_name
    ),

    monthly_partner_with_template_offers as (
        select
            partner_region_name,
            partner_department_name,
            date_trunc(first_template_offer_creation_date, month) as partition_month,
            count(distinct partner_id) as monthly_new_partners_with_template_offers
        from partner_with_template_offers
        group by partition_month, partner_region_name, partner_department_name
    ),

    cumul_partner_template as (
        select
            partition_month,
            partner_region_name,
            partner_department_name,
            coalesce(sum(monthly_new_partners_with_template_offers) over (
                partition by partner_region_name, partner_department_name
                order by partition_month asc
            ),0) as cumul_partners_with_template_offers
        from monthly_partner_with_template_offers
    )

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(date(partition_day), month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'pct_partenaire_culturel_actif' as kpi_name,
        coalesce(count(
            distinct case
                when
                    days_since_last_collective_bookable_date
                    <= date_diff(partition_day, educational_year_beginning_date, day)
                then partner_id
            end
        ),0) as numerator,
        coalesce(count(
            distinct case
                when days_since_last_collective_bookable_date >= 0 then partner_id
            end
        ),0) as denominator,
        safe_divide(
            coalesce(count(
                distinct case
                    when
                        days_since_last_collective_bookable_date <= date_diff(
                            partition_day, educational_year_beginning_date, day
                        )
                    then partner_id
                end
            ),0),
            coalesce(count(
                distinct case
                    when days_since_last_collective_bookable_date >= 0 then partner_id
                end
            ),0)
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
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'total_partenaire_avec_offre_vitrine' as kpi_name,
        coalesce(sum(cumul_partners_with_template_offers),0) as numerator,
        1 as denominator,
        coalesce(sum(cumul_partners_with_template_offers),0) as kpi
    from cumul_partner_template
    {% if is_incremental() %}
        where
            partition_month
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value
{% endfor %}
