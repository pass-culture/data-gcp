with
    temp1 as (
        select
            format_date("%Y-%m-%d", month) as date_valeur,
            case
                when dimension_value = 'NAT' then 'FRANCE' else dimension_value
            end as dimension_value,
            dep_name,
            dimension_name,
            "va" as type_valeur,
            case
                when indicator = "taux_couverture" and user_type = "19"
                then "IND-200"
                when indicator = "taux_couverture" and user_type = "17"
                then "IND-199"
                when
                    indicator = "montant_depense_24_month"
                    and user_type = "GRANT_18"
                    and month > "2023-05-01"
                then "IND-202"
                when indicator = "taux_participation_eac_jeunes"
                then "IND-205"
                when indicator = "taux_retention_partenaires"
                then "IND-201"
                when indicator = "taux_participation_eac_ecoles"
                then "IND-046"
            end as identifiant_indic,
            sum(numerator) as numerator,
            sum(denominator) as denominator
        from `{{ bigquery_analytics_dataset }}`.propilote_kpis
        left join
            `{{ bigquery_analytics_dataset }}`.region_department
            on propilote_kpis.dimension_value = region_department.num_dep
        where dimension_name != 'ACAD' and is_current_year
        group by 1, 2, 3, 4, 5, 6

    ),
    temp2 as (
        select
            identifiant_indic,
            case
                when dep_name is not null then dep_name else dimension_value
            end as dimension_value,
            date_valeur,
            type_valeur,
            case
                when identifiant_indic = 'IND-202'
                then safe_divide(numerator, denominator)
                when
                    identifiant_indic != 'IND-202'
                    and round(safe_divide(numerator, denominator) * 100, 2) > 100
                then 100
                else round(safe_divide(numerator, denominator) * 100, 2)
            end as valeur
        from temp1
        where identifiant_indic is not null and dimension_value is not null
    )
select distinct
    identifiant_indic,
    case
        when temp2.dimension_value = 'FRANCE'
        then temp2.dimension_value
        else pilote_geographic_standards.zone_id
    end as zone_id,
    date_valeur,
    type_valeur,
    valeur
from temp2
left join
    `{{ bigquery_analytics_dataset }}`.pilote_geographic_standards
    on temp2.dimension_value = pilote_geographic_standards.nom
where
    (
        pilote_geographic_standards.zone_id is not null
        or temp2.dimension_value = 'FRANCE'
    )
