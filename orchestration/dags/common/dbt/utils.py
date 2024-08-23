import json
from typing import Dict, List, Optional, Tuple

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup


def get_models_folder_dict(dbt_models: List, manifest: Optional[Dict]) -> Dict:
    """Return a dictionnary associating dbt nodes in "model" folder to the list of folder containing it
    Exemple output:
    {model2_id:[folder_A,subfolder_A],model2_id:[folder_A,subfolder_B,subsubfolderB],model3_id:[folder_B]}
    """
    return {
        model_node: manifest["nodes"][model_node]["fqn"][1:-1]
        for model_node in dbt_models
    }


def create_nested_folder_groups(
    dbt_models: List, model_op_dict: Dict, manifest: Optional[Dict], dag: DAG
):
    nested_folders = get_models_folder_dict(dbt_models, manifest)
    task_groups = {}

    def create_nested_task_group(
        folder_hierarchy: List[str],
        original_hierarchy: List[str],
        parent_group: Optional[TaskGroup] = None,
        current_path: str = "",
    ) -> Tuple[Optional[TaskGroup], List[str]]:
        """
        Recursively create nested TaskGroups based on folder hierarchy.
        This function modifies task_group dictionnary declared above
        TODO:
            - remove leaf TaskGroup and put the trigger operator in parent TaskGroup
            hint: the recursive function is nihilpotent over original_hierarchy
            - internalize task_group dict and output it
        """
        if not folder_hierarchy:
            return (None, original_hierarchy)

        current_folder = folder_hierarchy[0]

        # Build the full path for the group_id
        current_path = (
            f"{current_path}_{current_folder}" if current_path else current_folder
        )
        group_id = current_path

        # Check if the group already exists
        if group_id in task_groups:
            tg, dummy_task = task_groups[group_id]
        else:
            # Create task group & corresponding trigger opperator
            tg = TaskGroup(group_id=group_id, parent_group=parent_group, dag=dag)
            dummy_task = DummyOperator(
                task_id=f"trigger_{group_id}_folder", task_group=tg, dag=dag
            )
            # Add them to dictionnary
            task_groups[group_id] = (tg, dummy_task)

        # Recurse to create the nested TaskGroups
        if len(folder_hierarchy) > 1:
            _, original_hierarchy = create_nested_task_group(
                folder_hierarchy[1:],
                original_hierarchy,
                parent_group=tg,
                current_path=current_path,
            )

        return (tg, original_hierarchy)

    # Create task groups for each folder hierarchy in nested_folders
    for folder_hierarchy in nested_folders.values():
        original_hierarchy = folder_hierarchy.copy()
        create_nested_task_group(folder_hierarchy, original_hierarchy)

    # Set the upstream for each task in model_op_dict to the corresponding trigger operator
    for model_node, folder_hierarchy in nested_folders.items():
        # Chain trigger tasks of folder hierarchy
        task_chain = [
            task_groups["_".join(folder_hierarchy[: i + 1])][1]
            for i, _ in enumerate(folder_hierarchy)
        ]
        chain(*task_chain)
        # Set model execution task's upstream to leaf folder trigger task
        task_chain[-1] >> model_op_dict[model_node]


def load_json_artifact(_PATH_TO_DBT_TARGET, artifact):
    local_filepath = _PATH_TO_DBT_TARGET + "/" + artifact
    try:
        with open(local_filepath) as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}
    return data


def load_manifest(_PATH_TO_DBT_TARGET):
    manifest = load_json_artifact(_PATH_TO_DBT_TARGET, "manifest.json")
    if manifest != {}:
        return manifest
    else:
        empty_manifest = {
            "metadata": {},
            "nodes": {},
            "sources": {},
            "metrics": {},
            "exposures": {},
            "groups": {},
            "macros": {},
            "docs": {},
            "parent_map": {},
            "child_map": {},
            "group_map": {},
            "selectors": {},
            "disabled": {},
        }
        return empty_manifest


def build_simplified_manifest(json_dict_data):
    simplified_manifest_models = {
        node: {
            "redirect_dep": None,
            "model_node": node,
            "model_alias": json_dict_data["nodes"][node]["alias"],
            "depends_on_node": json_dict_data["nodes"][node]["depends_on"]["nodes"],
            "model_tests": {},
            "resource_type": json_dict_data["nodes"][node]["resource_type"],
        }
        for node in json_dict_data["nodes"].keys()
        if (
            json_dict_data["nodes"][node]["resource_type"] == "model"
            and "data_gcp_dbt" in node
        )
    }

    simplified_manifest_sources = {
        node: {
            "redirect_dep": None,
            "model_node": node,
            "model_alias": json_dict_data["sources"][node]["name"],
            "depends_on_node": None,
            "model_tests": {},
            "resource_type": json_dict_data["sources"][node]["resource_type"],
        }
        for node in json_dict_data["sources"].keys()
        if (
            json_dict_data["sources"][node]["resource_type"] == "source"
            and "data_gcp_dbt" in node
        )
    }

    simplified_manifest = {**simplified_manifest_models, **simplified_manifest_sources}

    for node in json_dict_data["nodes"].keys():
        if json_dict_data["nodes"][node]["resource_type"] == "test":
            generic_test = True in [
                generic_name in node
                for generic_name in [
                    "not_null",
                    "unique",
                    "accepted_values",
                    "relationships",
                ]
            ]
            test_alias = (
                json_dict_data["nodes"][node]["alias"]
                if not generic_test
                else node.split(".")[-2]
            )
            test_config = json_dict_data["nodes"][node]["config"].get("severity", None)
            try:
                test_config = test_config.lower()
            except AttributeError:
                pass
            parents = json_dict_data["nodes"][node]["depends_on"]["nodes"]
            for p_node in parents:
                if (
                    simplified_manifest[p_node]["model_tests"].get(test_config, None)
                    is None
                ):
                    simplified_manifest[p_node]["model_tests"][test_config] = [
                        {
                            "test_alias": test_alias,
                            "test_node": node,
                            "test_type": "generic" if generic_test else "custom",
                        }
                    ]
                else:
                    simplified_manifest[p_node]["model_tests"][test_config] += [
                        {
                            "test_alias": test_alias,
                            "test_node": node,
                            "test_type": "generic" if generic_test else "custom",
                        }
                    ]

    return simplified_manifest


def rebuild_manifest(_PATH_TO_DBT_TARGET):
    try:
        json_dict_data = load_manifest(_PATH_TO_DBT_TARGET)
        simplified_manifest = build_simplified_manifest(json_dict_data)
    except FileNotFoundError:
        simplified_manifest = {}
    return simplified_manifest


def load_run_results(_PATH_TO_DBT_TARGET):
    json_dict_data = load_json_artifact(_PATH_TO_DBT_TARGET, "run_results.json")
    dict_results = {}
    for item in json_dict_data["results"]:
        dict_results[item["unique_id"]] = {
            **dict_results.get(item["unique_id"], {}),
            **item,
        }
    return dict_results
