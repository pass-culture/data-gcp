select * from `{{ bigquery_tmp_dataset }}.{{ params.tmp_table_prefix }}_{{ ds_nodash }}`
