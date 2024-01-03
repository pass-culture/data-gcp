{% macro create_humanize_id_function() %}

{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_dbt_' ~ target_name) %}

CREATE FUNCTION if not exists {{ target_schema }}.humanize_id(id STRING)
RETURNS STRING
LANGUAGE js
OPTIONS (library="gs://data-bucket-{{ target_name }}/base32-encode/base32.js")
AS 
"""
// turn int into bytes array
var byteArray = [];
var updated_id = id;
while (updated_id != 0) {
    var byte = updated_id & 0xff;
    byteArray.push(byte);
    updated_id = (updated_id - byte) / 256 ;
}
var reversedByteArray = byteArray.reverse();
// apply base32 encoding
var raw_b32 = base32Encode(new Uint8Array(reversedByteArray), 'RFC4648', { padding: false });
// replace "O" with "8" and "I" with "9"
return raw_b32.replace(/O/g, '8').replace(/I/g, '9')
""";

{% endmacro %}
