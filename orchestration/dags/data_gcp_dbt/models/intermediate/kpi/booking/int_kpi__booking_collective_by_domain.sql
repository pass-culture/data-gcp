/* Intermediate KPI aggregating confirmed collective bookings by educational domain and institution geography. */
{% set geo_columns = [
    "institution_region_name",
    "institution_region_code",
    "institution_department_name",
    "institution_department_code",
    "institution_epci_name",
    "institution_epci_code",
    "institution_city_code",
] %}
{% set measures = [
    "total_collective_bookings",
    "total_collective_booking_amount",
    "total_collective_tickets",
    "new_collective_institutions",
] %}

with
    bookings as (
        select
            cb.collective_booking_id,
            cb.educational_institution_id,
            cb.booking_amount,
            cb.collective_stock_number_of_tickets,
            cb.scholar_year,
            cod.educational_domain_name,
            cb.institution_region_name,
            coalesce(cast(rd.region_code as string), '-1') as institution_region_code,
            cb.institution_department_name,
            cb.institution_department_code,
            cb.institution_epci as institution_epci_name,
            cb.institution_epci_code,
            cb.institution_city_code,
            -- bookings created before their scholar year starts are attributed to
            -- its first month, so that Sep-Aug sums match the scholar year
            {{
                scholar_year_partition_month(
                    "cb.collective_booking_creation_date", "ey"
                )
            }} as partition_month
        from {{ ref("mrt_global__collective_booking") }} as cb
        inner join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on cb.scholar_year = ey.scholar_year
        left join
            {{ ref("mrt_global__collective_offer_domain") }} as cod
            on cb.collective_offer_id = cod.collective_offer_id
        left join
            {{ ref("region_department") }} as rd
            on cb.institution_department_code = rd.num_dep
        where
            cb.collective_booking_status
            in ('CONFIRMED', 'USED', 'PENDING_REIMBURSEMENT', 'REIMBURSED')
    ),

    bookings_with_key as (
        select
            *,
            to_json_string(
                struct(educational_domain_name, {{ geo_columns | join(", ") }})
            ) as cell_key,
            -- an institution is new in a domain on its first booking month of
            -- the scholar year, so the running sum is a distinct count
            partition_month = min(partition_month) over (
                partition by
                    scholar_year, educational_domain_name, educational_institution_id
            ) as is_institution_first_month
        from bookings
    ),

    monthly as (
        select
            scholar_year,
            cell_key,
            partition_month,
            count(collective_booking_id) as total_collective_bookings,
            sum(booking_amount) as total_collective_booking_amount,
            sum(collective_stock_number_of_tickets) as total_collective_tickets,
            count(distinct educational_institution_id) as total_collective_institutions,
            count(
                distinct if(
                    is_institution_first_month, educational_institution_id, null
                )
            ) as new_collective_institutions
        from bookings_with_key
        group by scholar_year, cell_key, partition_month
    ),

    cells as (
        select distinct
            scholar_year,
            cell_key,
            educational_domain_name,
            {{ geo_columns | join(", ") }}
        from bookings_with_key
    ),

    grid as (
        select cells.*, partition_month
        from cells
        inner join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on cells.scholar_year = ey.scholar_year
        cross join
            unnest(
                generate_date_array(
                    date(ey.educational_year_beginning_date),
                    date(ey.educational_year_expiration_date),
                    interval 1 month
                )
            ) as partition_month
    ),

    complete as (
        select
            grid.*,
            coalesce(
                monthly.total_collective_institutions, 0
            ) as total_collective_institutions,
            {% for measure in measures %}
                coalesce(monthly.{{ measure }}, 0) as {{ measure }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from grid
        left join
            monthly
            on grid.scholar_year = monthly.scholar_year
            and grid.cell_key = monthly.cell_key
            and grid.partition_month = monthly.partition_month
    )

select
    partition_month,
    scholar_year,
    educational_domain_name,
    {{ geo_columns | join(",\n    ") }},
    total_collective_bookings,
    total_collective_booking_amount,
    total_collective_tickets,
    total_collective_institutions,
    sum(total_collective_bookings) over w as cumulative_total_collective_bookings,
    sum(total_collective_booking_amount) over w
    as cumulative_total_collective_booking_amount,
    sum(total_collective_tickets) over w as cumulative_total_collective_tickets,
    sum(new_collective_institutions) over w as cumulative_total_collective_institutions
from complete
window
    w as (
        partition by scholar_year, cell_key
        order by partition_month
        rows unbounded preceding
    )
