## Clusterisation

This folder contains the code and documentation for the clusterisation jobs of items based on JSON configuration files.

To run the clusterisation job, follow these steps:

See the airflow job for table configurations.

1. Run the preprocessing:

```sh
python cluster/preprocess.py --input-table <input_table> --output-table <output_table> --config-file-name <config_file_name> --cluster-prefix <cluster_prefix>
```

2. Run the cluster generation job:

```sh
python cluster/generate.py --input-table <input_table> --output-table <output_table> --config-file-name <config_file_name> --cluster-prefix <cluster_prefix>
```
