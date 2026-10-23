{% macro collective_bookable_offer_kpis(geo_side) %}
    {#
        Monthly KPIs on bookable (non template) collective offers.
        Args:
            geo_side: "venue" (cultural partner location) or "institution"
                (location of the school the offer is made for). Output geo
                columns are prefixed with it.

        Rules:
            - an offer belongs to the scholar year of its action (stock
              beginning date); its creation month is clamped into that
              scholar year, so Sep-Aug sums give scholar year totals;
            - one row per scholar year month x geography seen in that scholar
              year, so cumulative_* columns exist for every month;
            - distinct partners are only counted on the venue side, where a
              partner sits in one geography.
    #}
    {% set geo_columns = [
        "region_name",
        "region_code",
        "academy_name",
        "department_name",
        "department_code",
        "epci_name",
        "epci_code",
        "city_name",
        "city_code",
        "municipality_label",
        "municipality_code",
        "macro_density_label",
    ] %}
    {% set measures = [
        "total_created_bookable_collective_offers",
        "total_prereserved_bookable_collective_offers",
    ] %}
    {% if geo_side == "venue" %}
        {% set measures = measures + ["new_bookable_offer_partners"] %}
        {% set geo_alias = "co" %}
    {% else %} {% set geo_alias = "inst" %}
    {% endif %}

    with
        offers as (
            select
                mco.collective_offer_id,
                mco.venue_id,
                mco.total_non_cancelled_collective_bookings > 0 as is_prereserved,
                ey.scholar_year,
                {{ geo_alias }}.{{ geo_side }}_region_name,
                coalesce(
                    cast(rd.region_code as string), '-1'
                ) as {{ geo_side }}_region_code,
                {{ geo_alias }}.{{ geo_side }}_academy_name,
                {{ geo_alias }}.{{ geo_side }}_department_name,
                {{ geo_alias }}.{{ geo_side }}_department_code,
                {{ geo_alias }}.{{ geo_side }}_epci as {{ geo_side }}_epci_name,
                {{ geo_alias }}.{{ geo_side }}_epci_code,
                {{ geo_alias }}.{{ geo_side }}_city as {{ geo_side }}_city_name,
                {{ geo_alias }}.{{ geo_side }}_city_code,
                {{ geo_alias }}.{{ geo_side }}_municipality_label,
                {{ geo_alias }}.{{ geo_side }}_municipality_code,
                {{ geo_alias }}.{{ geo_side }}_macro_density_label,
                {{
                    scholar_year_partition_month(
                        "mco.collective_offer_creation_date", "ey"
                    )
                }} as partition_month
            from {{ ref("mrt_global__collective_offer") }} as mco
            inner join
                {{ source("raw", "applicative_database_educational_year") }} as ey
                on mco.collective_stock_beginning_date_time
                between ey.educational_year_beginning_date
                and ey.educational_year_expiration_date
            {% if geo_side == "venue" %}
                inner join
                    {{ ref("int_global__collective_offer") }} as co
                    on mco.collective_offer_id = co.collective_offer_id
            {% else %}
                -- left join: offers whose school is missing from the mart are
                -- kept in an unknown geography, so both sides have the same totals
                left join
                    {{ ref("mrt_global__educational_institution") }} as inst
                    on mco.institution_id = inst.institution_id
            {% endif %}
            left join
                {{ source("seed", "region_department") }} as rd
                on {{ geo_alias }}.{{ geo_side }}_department_code = rd.num_dep
            where not mco.collective_offer_is_template
        ),

        offers_with_geo_key as (
            select
                *,
                to_json_string(
                    struct(
                        {% for col in geo_columns %}
                            {{ geo_side }}_{{ col }}{% if not loop.last %},{% endif %}
                        {% endfor %}
                    )
                ) as geo_key,
                -- a partner is new on its first bookable offer month of the
                -- scholar year, so the running sum is a distinct count
                partition_month = min(partition_month) over (
                    partition by scholar_year, venue_id
                ) as is_partner_first_month
            from offers
        ),

        monthly as (
            select
                scholar_year,
                geo_key,
                partition_month,
                count(collective_offer_id) as total_created_bookable_collective_offers,
                countif(is_prereserved) as total_prereserved_bookable_collective_offers,
                count(
                    distinct if(is_partner_first_month, venue_id, null)
                ) as new_bookable_offer_partners
            from offers_with_geo_key
            group by scholar_year, geo_key, partition_month
        ),

        geographies as (
            select distinct
                scholar_year,
                geo_key,
                {% for col in geo_columns %}
                    {{ geo_side }}_{{ col }}{% if not loop.last %},{% endif %}
                {% endfor %}
            from offers_with_geo_key
        ),

        grid as (
            select geographies.*, partition_month
            from geographies
            inner join
                {{ source("raw", "applicative_database_educational_year") }} as ey
                on geographies.scholar_year = ey.scholar_year
            cross join
                unnest(
                    generate_date_array(
                        date(ey.educational_year_beginning_date),
                        date(ey.educational_year_expiration_date),
                        interval 1 month
                    )
                ) as partition_month
        )

    select
        grid.partition_month,
        grid.scholar_year,
        {% for col in geo_columns %} grid.{{ geo_side }}_{{ col }}, {% endfor %}
        {% for measure in measures %}
            coalesce(monthly.{{ measure }}, 0) as {{ measure }},
        {% endfor %}
        {% for measure in measures %}
            sum(coalesce(monthly.{{ measure }}, 0)) over (
                partition by grid.scholar_year, grid.geo_key
                order by grid.partition_month
                rows unbounded preceding
            ) as cumulative_{{ measure }}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    from grid
    left join
        monthly
        on grid.scholar_year = monthly.scholar_year
        and grid.geo_key = monthly.geo_key
        and grid.partition_month = monthly.partition_month
{% endmacro %}
