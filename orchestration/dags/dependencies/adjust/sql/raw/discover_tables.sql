select table_name
from `{{ source_project }}.{{ source_dataset }}.INFORMATION_SCHEMA.TABLES`
where table_type = 'BASE TABLE' and starts_with(table_name, @table_prefix)
order by table_name
