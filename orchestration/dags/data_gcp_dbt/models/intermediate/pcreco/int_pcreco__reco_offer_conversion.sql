{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            cluster_by="reco_call_id",
            require_partition_filter=true,
        )
    )
}}


select
    event_date,
    reco_config_id,
    reco_call_id,
    offer_id,
    item_id,
    offer_category,
    offer_reco_rank,
    offer_viewed,
    offer_consulted,
    offer_faved,
    offer_booked,
    discovery_score,
    diversity_score,
    item_niche_score,
    offer_niche_score,
    offer_booking_number
from {{ ref("int_pcreco__displayed_offer_conversion") }}
{% if is_incremental() %}
    where
        event_date
        between date_sub(date('{{ ds() }}'), interval 3 day) and date('{{ ds() }}')
{% else %} where event_date >= date_sub(date('{{ ds() }}'), interval 60 day)
{% endif %}
