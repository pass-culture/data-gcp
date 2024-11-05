select * from {{ ref("snapshot_raw__deposit") }} where {{ var("snapshot_filter") }}
