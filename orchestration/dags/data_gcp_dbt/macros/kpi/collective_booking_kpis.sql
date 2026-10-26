{% macro collective_booking_kpis(geo_side) %}
    {#
        Monthly collective booking KPIs for one geography side of the booking.
        Args:
            geo_side: "venue" (cultural partner location) or "institution"
                (school location). Output geo columns are prefixed with it.

        Rules:
            - non-cancelled bookings only;
            - every event month (pre-reservation, confirmation, action) is
              clamped into the booking's scholar year, so Sep-Aug sums give
              scholar year totals;
            - one row per scholar year month x geography seen in that scholar
              year, so cumulative_* columns exist for every month.
    #}
    {% set confirmed_statuses = (
        "'CONFIRMED', 'USED', 'PENDING_REIMBURSEMENT', 'REIMBURSED'"
    ) %}
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
        "total_prereservations",
        "total_prereservation_amount",
        "total_collective_bookings",
        "total_collective_amount_spent",
        "total_confirmations",
        "total_confirmation_amount",
        "total_actions",
        "total_action_amount",
    ] %}

    with
        bookings as (
            select
                cb.collective_booking_id,
                cb.booking_amount,
                cb.scholar_year,
                cb.collective_booking_status
                in ({{ confirmed_statuses }}) as is_confirmed,
                cb.{{ geo_side }}_region_name,
                coalesce(
                    cast(rd.region_code as string), '-1'
                ) as {{ geo_side }}_region_code,
                cb.{{ geo_side }}_academy_name,
                cb.{{ geo_side }}_department_name,
                cb.{{ geo_side }}_department_code,
                cb.{{ geo_side }}_epci as {{ geo_side }}_epci_name,
                cb.{{ geo_side }}_epci_code,
                cb.{{ geo_side }}_city as {{ geo_side }}_city_name,
                cb.{{ geo_side }}_city_code,
                cb.{{ geo_side }}_municipality_label,
                cb.{{ geo_side }}_municipality_code,
                cb.{{ geo_side }}_macro_density_label,
                {{
                    scholar_year_partition_month(
                        "cb.collective_booking_creation_date", "ey"
                    )
                }} as creation_month,
                if(
                    cb.collective_booking_confirmation_date is null,
                    null,
                    {{
                        scholar_year_partition_month(
                            "cb.collective_booking_confirmation_date", "ey"
                        )
                    }}
                ) as confirmation_month,
                {{
                    scholar_year_partition_month(
                        "cb.collective_stock_beginning_date_time", "ey"
                    )
                }} as action_month
            from {{ ref("mrt_global__collective_booking") }} as cb
            inner join
                {{ source("raw", "applicative_database_educational_year") }} as ey
                on cb.scholar_year = ey.scholar_year
            left join
                {{ source("seed", "region_department") }} as rd
                on cb.{{ geo_side }}_department_code = rd.num_dep
            where cb.collective_booking_status != 'CANCELLED'
        ),

        bookings_with_geo_key as (
            select
                *,
                to_json_string(
                    struct(
                        {% for col in geo_columns %}
                            {{ geo_side }}_{{ col }}{% if not loop.last %},{% endif %}
                        {% endfor %}
                    )
                ) as geo_key
            from bookings
        ),

        monthly as (
            select
                b.scholar_year,
                b.geo_key,
                e.event_month as partition_month,
                countif(e.event = 'creation') as total_prereservations,
                sum(
                    if(e.event = 'creation', b.booking_amount, 0)
                ) as total_prereservation_amount,
                countif(
                    e.event = 'creation' and b.is_confirmed
                ) as total_collective_bookings,
                sum(
                    if(e.event = 'creation' and b.is_confirmed, b.booking_amount, 0)
                ) as total_collective_amount_spent,
                countif(
                    e.event = 'confirmation' and b.is_confirmed
                ) as total_confirmations,
                sum(
                    if(e.event = 'confirmation' and b.is_confirmed, b.booking_amount, 0)
                ) as total_confirmation_amount,
                countif(e.event = 'action' and b.is_confirmed) as total_actions,
                sum(
                    if(e.event = 'action' and b.is_confirmed, b.booking_amount, 0)
                ) as total_action_amount
            from bookings_with_geo_key as b
            cross join
                unnest(
                    [
                        struct('creation' as event, b.creation_month as event_month),
                        struct('confirmation', b.confirmation_month),
                        struct('action', b.action_month)
                    ]
                ) as e
            where e.event_month is not null
            group by b.scholar_year, b.geo_key, partition_month
        ),

        geographies as (
            select distinct
                scholar_year,
                geo_key,
                {% for col in geo_columns %}
                    {{ geo_side }}_{{ col }}{% if not loop.last %},{% endif %}
                {% endfor %}
            from bookings_with_geo_key
        ),

        scholar_year_months as (
            select ey.scholar_year, partition_month
            from {{ source("raw", "applicative_database_educational_year") }} as ey
            cross join
                unnest(
                    generate_date_array(
                        date(ey.educational_year_beginning_date),
                        date(ey.educational_year_expiration_date),
                        interval 1 month
                    )
                ) as partition_month
        ),

        grid as (
            select geographies.*, scholar_year_months.partition_month
            from geographies
            inner join
                scholar_year_months
                on geographies.scholar_year = scholar_year_months.scholar_year
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
