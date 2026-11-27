{#
    List of the cultural partners (venues) active in the current month: a
    bookable individual or collective offer in the last 365 days. Same rule,
    same filters and same partner types as int_kpi__cultural_partner_activation
    (_by_type), so that counting this list gives the latest month of
    exp_vidoc__cultural_partner and, per is_<type> flag, of
    exp_vidoc__cultural_partner_by_type. No amounts: partner lists are public,
    revenue is under the business secret.
#}
{% set partner_types = get_partner_types() %}

with
    last_bookable_dates as (
        select
            gcp.venue_id,
            max(
                case
                    when h.total_individual_bookable_offers > 0 then h.partition_date
                end
            ) as last_individual_bookable_date,
            max(
                case
                    when h.total_collective_bookable_offers > 0 then h.partition_date
                end
            ) as last_collective_bookable_date
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        inner join
            {{ ref("int_history__bookable_venue") }} as h
            on gcp.venue_id = h.venue_id
            and h.partition_date <= current_date()
        where
            gcp.first_individual_offer_creation_date is not null
            or gcp.first_collective_offer_creation_date is not null
        group by gcp.venue_id
    ),

    active_partners as (
        select
            venue_id,
            last_individual_bookable_date,
            last_collective_bookable_date,
            coalesce(
                date_diff(current_date(), last_individual_bookable_date, day) <= 365,
                false
            ) as is_active_individual,
            coalesce(
                date_diff(current_date(), last_collective_bookable_date, day) <= 365,
                false
            ) as is_active_collective
        from last_bookable_dates
    ),

    -- one row per venue and label (category 1 tags), as in the counts
    partner_details as (
        select gcp.venue_id, gcp.partner_type, gvt.venue_tag_name
        from active_partners as ap
        inner join
            {{ ref("mrt_global__cultural_partner") }} as gcp
            on ap.venue_id = gcp.venue_id
        left join
            {{ ref("mrt_global__venue_tag") }} as gvt
            on gcp.venue_id = gvt.venue_id
            and gvt.venue_tag_category_id = '1'
        where ap.is_active_individual or ap.is_active_collective
    ),

    partner_types_and_labels as (
        select
            venue_id,
            string_agg(
                distinct venue_tag_name, ', ' order by venue_tag_name
            ) as venue_labels,
            {% for partner_type in partner_types %}
                logical_or(
                    coalesce({{ partner_type.condition }}, false)
                ) as is_{{ partner_type.name }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from partner_details
        group by venue_id
    )

select
    date_trunc(current_date(), month) as partition_month,
    gcp.venue_id,
    gcp.partner_name as venue_name,
    gof.offerer_id,
    gof.offerer_name,
    gcp.partner_type as venue_type_label,
    ptl.venue_labels,
    ap.is_active_individual,
    ap.is_active_collective,
    ap.is_active_individual and ap.is_active_collective as is_active_dual_part,
    ap.last_individual_bookable_date,
    ap.last_collective_bookable_date,
    gcp.partner_region_name,
    gcp.partner_region_code,
    gcp.partner_department_name,
    gcp.partner_department_code,
    gcp.partner_epci_code,
    gcp.partner_municipality_code,
    gcp.partner_municipality_label,
    gcp.partner_postal_code,
    gcp.cultural_partner_is_in_qpv as partner_is_in_qpv,
    {% for partner_type in partner_types %}
        ptl.is_{{ partner_type.name }}{% if not loop.last %},{% endif %}
    {% endfor %}
from active_partners as ap
inner join partner_types_and_labels as ptl on ap.venue_id = ptl.venue_id
inner join
    {{ ref("mrt_global__cultural_partner") }} as gcp on ap.venue_id = gcp.venue_id
inner join {{ ref("mrt_global__offerer") }} as gof on gcp.offerer_id = gof.offerer_id
where gcp.partner_municipality_code is not null
