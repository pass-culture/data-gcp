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

-- Définition des types de partenaires culturels avec leurs critères
{% set partner_types = [
    {
        "name": "cinemas",
        "label": "Cinémas",
        "condition": "partner_type IN ('Cinéma - Salle de projections', 'Cinéma itinérant') OR venue_tag_name IN (\"Cinéma d'art et d'essai\")",
    },
    {
        "name": "cinemas_art_et_essai",
        "label": "Cinémas Art et Essai",
        "condition": "venue_tag_name IN (\"Cinéma d'art et d'essai\")",
    },
    {
        "name": "librairies",
        "label": "Librairies",
        "condition": "partner_type IN ('Librairie', 'Magasin de grande distribution')",
    },
    {
        "name": "spectacle_vivant",
        "label": "Spectacle Vivant",
        "condition": "partner_type IN ('Spectacle vivant')",
    },
    {
        "name": "spectacle_vivant_labels",
        "label": "Spectacle Vivant avec Labels",
        "condition": "partner_type IN ('Spectacle vivant') AND venue_tag_name IN ('CCN','CDCN','CDN','CNAREP','SCIN','Scène nationale','Théâtre lyrique','Théâtres nationaux')",
    },
    {
        "name": "musique_live",
        "label": "Musique Live",
        "condition": "partner_type IN ('Musique - Salle de concerts', 'Festival')",
    },
    {
        "name": "musee",
        "label": "Musées",
        "condition": "partner_type IN ('Musée')",
    },
    {
        "name": "musee_labels",
        "label": "Musées avec Labels",
        "condition": "partner_type IN ('Musée') AND venue_tag_name IN ('MdF')",
    },
] %}

with
    all_activated_partners_and_days as (
        -- Pour chaque partner_id, une ligne par jour depuis la 1ère offre publiée
        select
            gcp.partner_id,
            gcp.first_individual_offer_creation_date,
            gcp.first_collective_offer_creation_date,
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
            apd.partner_id,
            apd.first_individual_offer_creation_date,
            apd.first_collective_offer_creation_date,
            apd.partition_day,
            coalesce(bph.individual_bookable_offers, 0) as total_indiv_bookable_offers,
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
                        partition by partner_id
                        order by partition_day
                        rows between unbounded preceding and current row
                    ),
                    first_individual_offer_creation_date
                ),
                day
            ) as days_since_last_indiv_bookable_date,
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
            bd.first_individual_offer_creation_date,
            bd.days_since_last_indiv_bookable_date,
            bd.days_since_last_collective_bookable_date,
            gcp.partner_region_name,
            gcp.partner_department_name,
            gcp.partner_type,
            gcp.offerer_id,
            gvt.venue_tag_name
        from bookable_dates as bd
        inner join
            {{ ref("mrt_global__cultural_partner") }} as gcp
            on bd.partner_id = gcp.partner_id
        left join
            {{ ref("mrt_global__venue_tag") }} as gvt on gcp.partner_id = gvt.partner_id
        inner join
            {{ ref("mrt_global__offerer") }} as gof on gcp.offerer_id = gof.offerer_id
    )

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(date(partition_day), month) as partition_month,
        date("{{ ds() }}") as update_date,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'nombre_total_de_partenaire_actif' as kpi_name,
        count(
            distinct case
                when days_since_last_indiv_bookable_date <= 365 then partner_id
            end
        ) as numerator,
        1 as denominator,
        count(
            distinct case
                when days_since_last_indiv_bookable_date <= 365 then partner_id
            end
        ) as kpi
    from partner_details
    where
        1 = 1
        {% if is_incremental() %}
            and date_trunc(date(partition_day), month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, update_date, dimension_name, dimension_value, kpi_name
    union all
    {% for partner_type in partner_types %}
        {% if not loop.first %}
            union all
        {% endif %}
        select
            date_trunc(date(partition_day), month) as partition_month,
            date("{{ ds() }}") as update_date,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            "nombre_de_partenaire_actif_{{ partner_type.name }}" as kpi_name,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date <= 365
                        and {{ partner_type.condition }}
                    then partner_id
                end
            ) as numerator,
            1 as denominator,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date <= 365
                        and {{ partner_type.condition }}
                    then partner_id
                end
            ) as kpi
        from partner_details
        where
            1 = 1
            {% if is_incremental() %}
                and date_trunc(date(partition_day), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
        group by partition_month, update_date, dimension_name, dimension_value, kpi_name
    {% endfor %}
{% endfor %}
