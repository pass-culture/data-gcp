## Clusterisation

Les scripts de clusterisation sont exécutés via les DAG Airflow `orchestration/dags/item_clusterisation.py`

Le process de clusterisation est découpé en deux script python: 
* Le preprocess des embeddings précalculés dans le format adequat pour la clusterisation
``
python preprocess.py --input-table {table_with_embeddings} --output-table {input_table_for_clusterisation} --config-file-name {config_for_the_run}
``
NB: Le script est adapté pour que 'table_with_embeddings' contiennent les emb. dans une liste dans un string ex: ``"[1.22,3.45,-2.34,..]"``

* La clusterisation des items à proprement parlé :
``
python clusterisation.py --target-n-clusters --input-table --output-table --clustering-group
`` 
la notion de 'Group' est definis dans les fichier de config cf. `configs\` 