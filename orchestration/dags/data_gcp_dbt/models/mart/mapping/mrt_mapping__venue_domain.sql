{{ config(**custom_table_config(cluster_by=["educational_domain_name, venue_id"])) }}

select educational_domain_id, venue_id, educational_domain_name
from {{ ref("int_applicative__venue_domain") }}
