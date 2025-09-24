{% macro create_dehumanize_id_function() %}

    {% set target_name = target.name %}
    {% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

    create or replace function {{ target_schema }}.dehumanize_id(id string)
    returns string
    language js
    options
        (
            library
            = "gs://de-tools-{{ 'dev' if target_name == 'local' else target_name }}/base32-encode/base32.js"
        )
    as """
var public_id = id.replace(/8/g, 'O').replace(/9/g, 'I');
var byteArray = new Uint8Array(base32Decode(public_id, 'RFC4648')).reverse();
var value = 0;
for (var i = byteArray.length - 1; i >= 0; i--) {
    value = (value * 256) + byteArray[i];
}
return value;
"""
    ;

{% endmacro %}
