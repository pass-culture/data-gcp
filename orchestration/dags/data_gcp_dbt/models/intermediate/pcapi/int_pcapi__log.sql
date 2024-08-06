{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "partition_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
) }}

select
    DATE(timestamp) as partition_date,
    jsonpayload.extra.path as url_path,
    timestamp as log_timestamp,
    resource.labels.namespace_name as environement,
    jsonpayload.message,
    jsonpayload.technical_message_id,
    jsonpayload.extra.source,
    jsonpayload.extra.analyticssource as analytics_source,
    CAST(jsonpayload.user_id as INT64) as user_id,
    jsonpayload.extra.userid as extra_user_id,
    jsonpayload.extra.uai,
    jsonpayload.extra.user_role,
    jsonpayload.extra.from as origin,
    COALESCE(CAST(jsonpayload.extra.stockid as STRING), CAST(jsonpayload.extra.stock_id as STRING)) as stock_id,
    COALESCE(CAST(jsonpayload.extra.offerid as STRING), CAST(jsonpayload.extra.offer as STRING), CAST(jsonpayload.extra.offer_id as STRING)) as offer_id,
    CAST(jsonpayload.extra.collective_offer_template_id as STRING) as collective_offer_template_id,
    CAST(jsonpayload.extra.queryid as STRING) as query_id,
    jsonpayload.extra.comment,
    jsonpayload.extra.requested_date,
    CAST(jsonpayload.extra.total_students as INT) as total_students,
    CAST(jsonpayload.extra.total_teachers as INT) as total_teachers,
    jsonpayload.extra.header_link_name,
    CAST(COALESCE(jsonpayload.extra.bookingid, jsonpayload.extra.booking_id) as STRING) as booking_id,
    CAST(jsonpayload.extra.resultscount as INT) as results_count,
    CAST(jsonpayload.extra.resultnumber as INT) as results_number,
    jsonpayload.extra.filtervalues.eventaddresstype as address_type_filter,
    CAST(jsonpayload.extra.filtervalues.query as STRING) as text_filter,
    CAST(jsonpayload.extra.statuscode as INT64) as status_code,
    jsonpayload.extra.deviceid as device_id,
    jsonpayload.extra.sourceip as source_ip,
    jsonpayload.extra.appversion as app_version,
    jsonpayload.extra.platform,
    ARRAY_TO_STRING(jsonpayload.extra.filtervalues.departments, ',') as department_filter,
    ARRAY_TO_STRING(jsonpayload.extra.filtervalues.academies, ',') as academy_filter,
    ARRAY_TO_STRING(ARRAY(select CAST(value as STRING) from UNNEST(jsonpayload.extra.filtervalues.venue) as value), ',') as venue_filter,
    ARRAY_TO_STRING(ARRAY(select CAST(value as STRING) from UNNEST(jsonpayload.extra.filtervalues.domains) as value), ',') as artistic_domain_filter,
    ARRAY_TO_STRING(ARRAY(select CAST(value as STRING) from UNNEST(jsonpayload.extra.filtervalues.students) as value), ',') as student_filter,
    ARRAY_TO_STRING(jsonpayload.extra.filtervalues.formats, ',') as format_filter,
    jsonpayload.extra.suggestiontype as suggestion_type,
    jsonpayload.extra.suggestionvalue as suggestion_value,
    CAST(jsonpayload.extra.isfavorite as BOOLEAN) as is_favorite,
    CAST(jsonpayload.extra.playlistid as STRING) as playlist_id,
    CAST(jsonpayload.extra.domainid as STRING) as domain_id,
    COALESCE(CAST(jsonpayload.extra.venueid as STRING), CAST(jsonpayload.extra.venue as STRING), CAST(jsonpayload.extra.venue_id as STRING)) as venue_id,
    CAST(jsonpayload.extra.index as INT) as rank_clicked,
    COALESCE(CAST(jsonpayload.extra.product as STRING), CAST(jsonpayload.extra.product_id as STRING)) as product_id,
    CAST(jsonpayload.extra.old_quantity as INT64) as stock_old_quantity,
    CAST(jsonpayload.extra.stock_quantity as INT64) as stock_new_quantity,
    jsonpayload.extra.old_price as stock_old_price,
    jsonpayload.extra.stock_price as stock_new_price,
    CAST(jsonpayload.extra.stock_dnbookedquantity as INT64) as stock_booking_quantity,
    jsonpayload.extra.eans as list_of_eans_not_found,
    CAST(jsonpayload.extra.offerer_id as INT64) as offerer_id,
    jsonpayload.extra.isconvenient as beta_test_new_nav_is_convenient,
    jsonpayload.extra.ispleasant as beta_test_new_nav_is_pleasant,
    jsonpayload.extra.comment as beta_test_new_nav_comment,
    trace


from {{ source("raw","stdout") }}
    {% if is_incremental() %}
        WHERE DATE(timestamp) between DATE_SUB(DATE("{{ ds() }}"), interval 2 day) and DATE("{{ ds() }}")
    {% endif %}
