/*
    Requests to create functions to transform ids into humanized ids in BigQuery.

    They need the file base32.js to be uploaded in GCS.
*/

CREATE OR REPLACE FUNCTION
    raw_dev.humanize_id(id INT64)
    RETURNS STRING
    LANGUAGE js OPTIONS ( library="gs://pass-culture-data/base32-encode/base32.js" ) AS """
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
        return raw_b32.replace(/O/g, '8').replace(/I/g, '9');
    """;


CREATE OR REPLACE FUNCTION
    raw_dev.dehumanize_id(id STRING)
    RETURNS STRING
    LANGUAGE js OPTIONS (library=["gs://pass-culture-data/base32-encode/base32.js"]) AS """
        var public_id = id.replace(/8/g, 'O').replace(/9/g, 'I');

        var byteArray = new Uint8Array(base32Decode(public_id, 'RFC4648')).reverse();

        var value = 0;
        for (var i = byteArray.length - 1; i >= 0; i--) {
            value = (value * 256) + byteArray[i];
        }
        return value;
    """;
