with consultation as (
    select
        *,
        lag(consulted_items) over (partition by user_id order by consultation_date) as last_consulted_items,
        lag(consulted_origins) over (partition by user_id order by consultation_date) as last_consulted_origins
    from {{ ref('user_item_consultation') }}
),

new_consultation as (
    select
        user_id,
        consultation_date,
        array(
            select * from consultation.consulted_items
            except distinct
            (select * from consultation.last_consulted_items)
        ) as new_consulted_items,
        array(
            select * from consultation.consulted_origins
            except distinct
            (select * from consultation.last_consulted_origins)
        ) as new_consulted_origins
    from consultation
),

new_consultation_item as (
    select
        user_id,
        consultation_date,
        new_items,
        offer_category_id,
        offer_subcategory_id,
        {% for feature in discovery_vars("discovery_features") %}
            case
                when consultation_date = min(consultation_date) over (partition by user_id, {{ feature }})
                    then 1
                else 0
            end as {{ feature }}_discovery
        {% if not loop.last -%} , {%- endif %}
        {% endfor %}
    from new_consultation, unnest(new_consulted_items) new_items
        left join {{ ref('item_metadata') }} metadata
            on new_items = metadata.item_id
),

new_consultation_origin as (
    select
        user_id,
        consultation_date,
        new_origins as new_consulted_origin,
        case
            when consultation_date = min(consultation_date) over (partition by user_id, new_origins)
                then 1
            else 0
        end as new_origins_discovery
    from new_consultation, unnest(new_consulted_origins) as new_origins
),

agg_new_consultation_origin as (
    select
        user_id,
        consultation_date,
        sum(new_origins_discovery) as discovery_origin
    from new_consultation_origin
    group by 1, 2
),

agg_new_consultation_item as (
    select
        user_id,
        consultation_date,
        {% for feature in discovery_vars("discovery_features") %}
            sum({{ feature }}_discovery) as discovery_{{ feature }}
        {% if not loop.last -%} , {%- endif %}
        {% endfor %}
    from new_consultation_item
    group by 1, 2
)

select
    coalesce(agg_new_consultation_item.user_id, agg_new_consultation_origin.user_id) as user_id,
    coalesce(agg_new_consultation_item.consultation_date, agg_new_consultation_origin.consultation_date) as consultation_date,
    coalesce(discovery_origin, 0) as discovery_origin,
    {% for feature in discovery_vars("discovery_features") %}
        coalesce(discovery_{{ feature }}, 0) as discovery_{{ feature }}
        {% if not loop.last -%} , {%- endif %}
    {% endfor %},
    {% for feature in discovery_vars("discovery_features") %}
        coalesce(discovery_{{ feature }}, 0)
        {% if not loop.last -%} + {%- endif %}
    {% endfor %} + coalesce(discovery_origin, 0)
        as discovery_delta
from agg_new_consultation_item
    full outer join agg_new_consultation_origin
        using (user_id, consultation_date)
