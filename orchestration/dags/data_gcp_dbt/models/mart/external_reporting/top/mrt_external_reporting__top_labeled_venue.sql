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
    {"name": "REG", "value_expr": "venue_region_name"},
    {"name": "DEP", "value_expr": "venue_department_name"},
] %}

with
    base_aggregation as (
        select
            date_trunc(date(bo.booking_used_date), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            bo.venue_id,
            bo.venue_name,
            bo.offerer_name,
            bo.venue_region_name,
            bo.venue_department_name,
            vt.venue_tag_name,
            sum(bo.booking_intermediary_amount) as total_venue_booking_amount
        from {{ ref("mrt_global__booking") }} as bo
        inner join
            {{ ref("mrt_global__venue_tag") }} as vt
            on bo.venue_id = vt.venue_id
            and vt.venue_tag_category_label
            = "Comptage partenaire label et appellation du MC"
        where
            bo.booking_is_used
            {% if is_incremental() %}
                and date_trunc(date(bo.booking_used_date), month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
        group by
            partition_month,
            updated_at,
            bo.venue_id,
            bo.venue_name,
            bo.offerer_name,
            bo.venue_region_name,
            bo.venue_department_name,
            vt.venue_tag_name
    ),

    all_dimensions as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                venue_id,
                venue_name,
                offerer_name,
                venue_tag_name,
                sum(total_venue_booking_amount) as total_venue_booking_amount,
                row_number() over (
                    partition by
                        partition_month
                        {% if not dim.name == "NAT" %},{{ dim.value_expr }} {% endif %}
                    order by sum(total_venue_booking_amount) desc
                ) as total_venue_booking_amount_ranked
            from base_aggregation
            group by
                partition_month,
                updated_at,
                dimension_name,
                dimension_value,
                venue_id,
                venue_name,
                offerer_name,
                venue_tag_name
            qualify
                row_number() over (
                    partition by
                        partition_month
                        {% if not dim.name == "NAT" %},{{ dim.value_expr }} {% endif %}
                    order by total_venue_booking_amount desc
                )
                <= 50
        {% endfor %}
    )

select
    partition_month,
    updated_at,
    dimension_name,
    dimension_value,
    venue_id,
    venue_name,
    offerer_name,
    venue_tag_name,
    total_venue_booking_amount_ranked
from all_dimensions
