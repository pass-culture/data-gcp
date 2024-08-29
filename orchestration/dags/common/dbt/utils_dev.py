import json
from typing import Dict, List, Optional, Tuple, Union

from common.config import (
    EXCLUDED_TAGS,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
)

from airflow import DAG
from airflow.models.baseoperator import BaseOperator, chain
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup


def load_json_artifact(_PATH_TO_DBT_TARGET: str, artifact: str) -> Dict:
    local_filepath = _PATH_TO_DBT_TARGET + "/" + artifact
    try:
        with open(local_filepath) as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}
    return data


def load_manifest(_PATH_TO_DBT_TARGET: str) -> Dict:
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


def build_simplified_manifest(
    json_dict_data: Dict,
) -> Dict[str, Dict[str, Union[str, None, List[str], Dict[str, List[Dict[str, str]]]]]]:
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


def rebuild_manifest(_PATH_TO_DBT_TARGET: str) -> Dict:
    try:
        json_dict_data = load_manifest(_PATH_TO_DBT_TARGET)
        simplified_manifest = build_simplified_manifest(json_dict_data)
    except FileNotFoundError:
        simplified_manifest = {}
    return simplified_manifest


def load_run_results(_PATH_TO_DBT_TARGET: str) -> Dict[str, Dict]:
    json_dict_data = load_json_artifact(_PATH_TO_DBT_TARGET, "run_results.json")
    dict_results = {}
    for item in json_dict_data["results"]:
        dict_results[item["unique_id"]] = {
            **dict_results.get(item["unique_id"], {}),
            **item,
        }
    return dict_results


def load_and_process_manifest(
    manifest_path: str,
) -> Tuple[
    Dict,
    List[str],
    List[str],
    List[str],
    List[str],
    List[Optional[str]],
    Dict[Optional[str], List[str]],
]:
    manifest = load_manifest(manifest_path)
    dbt_snapshots: List[str] = []
    dbt_models: List[str] = []
    dbt_crit_tests: List[str] = []

    for node in manifest["nodes"].keys():
        node_data = manifest["nodes"][node]
        if node_data["package_name"] == "data_gcp_dbt":
            if node_data["resource_type"] == "snapshot":
                dbt_snapshots.append(node)
            if node_data["resource_type"] == "model":
                dbt_models.append(node)
            if (
                node_data["resource_type"] == "test"
                and node_data["config"].get("severity", "warn").lower() == "error"
            ):
                dbt_crit_tests.append(node)

    models_with_dependencies: List[str] = [
        node for node in manifest["child_map"].keys() if node in dbt_models
    ]

    models_with_crit_test_dependencies: List[Optional[str]] = [
        manifest["nodes"][node].get("attached_node") for node in dbt_crit_tests
    ]

    crit_test_parents: Dict[Optional[str], List[str]] = {
        manifest["nodes"][test].get("attached_node", None): [
            parent
            for parent in set(manifest["parent_map"][test]).intersection(
                set(dbt_models)
            )
        ]
        for test in dbt_crit_tests
    }

    return (
        manifest,
        dbt_snapshots,
        dbt_models,
        dbt_crit_tests,
        models_with_dependencies,
        models_with_crit_test_dependencies,
        crit_test_parents,
    )


def create_model_operator(
    model_node: str, model_data: Dict, is_applicative: bool, dag: DAG
) -> BashOperator:
    full_ref_str = (
        " --full-refresh" if "{{ params.full_refresh|lower }}" == "true" else ""
    )
    exclusion_str = (
        " --exclude " + " ".join([f"tag:{item}" for item in EXCLUDED_TAGS])
        if len(EXCLUDED_TAGS) > 0
        else ""
    )
    return BashOperator(
        task_id=model_data["alias"] if is_applicative else model_data["name"],
        bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_run.sh ",
        env={
            "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
            "target": "{{ params.target }}",
            "model": f"{model_data['name']}",
            "full_ref_str": full_ref_str,
            "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
            "EXCLUSION": exclusion_str,
        },
        append_env=True,
        cwd=PATH_TO_DBT_PROJECT,
        dag=dag,
    )


def create_test_operator(model_node: str, model_data: Dict, dag: DAG) -> BashOperator:
    full_ref_str = " --full-refresh" if not "{{ params.full_refresh }}" else ""
    return BashOperator(
        task_id=model_data["alias"] + "_tests",
        bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_test_model.sh ",
        env={
            "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
            "target": "{{ params.target }}",
            "model": f"""{model_data['alias']}""",
            "full_ref_str": full_ref_str,
            "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
        },
        append_env=True,
        cwd=PATH_TO_DBT_PROJECT,
        dag=dag,
    )


def create_critical_tests_group(
    dag: DAG,
    dbt_models: List[str],
    manifest: Dict,
    models_with_crit_test_dependencies: List[Optional[str]],
) -> Dict[str, BashOperator]:
    test_op_dict: Dict[str, BashOperator] = {}
    if any(
        model_node in models_with_crit_test_dependencies for model_node in dbt_models
    ):
        with TaskGroup(group_id="critical_tests", dag=dag) as crit_test_group:
            for model_node in dbt_models:
                model_data = manifest["nodes"][model_node]
                if model_node in models_with_crit_test_dependencies:
                    test_op_dict[model_node] = create_test_operator(
                        model_node, model_data, dag
                    )
    return test_op_dict


def set_up_model_dependencies(
    dag: DAG,
    dbt_models: List[str],
    manifest: Dict,
    model_op_dict: Dict[str, BaseOperator],
    test_op_dict: Dict[str, BaseOperator],
    models_with_crit_test_dependencies: List[Optional[str]],
    compile_op: BaseOperator,
) -> None:
    for model_node in dbt_models:
        setup_dependencies(
            model_node,
            manifest,
            model_op_dict,
            test_op_dict,
            models_with_crit_test_dependencies,
            compile_op,
        )


def setup_dependencies(
    model_node: str,
    manifest: Dict,
    model_op_dict: Dict[str, BaseOperator],
    test_op_dict: Dict[str, BaseOperator],
    models_with_crit_test_dependencies: List[Optional[str]],
    final_op: BaseOperator,
) -> None:
    """
    Set up dependencies for the given model_node, ensuring that children tasks are
    correctly linked, and that critical tests are attached to their models.

    Parameters:
    - model_node: The model node for which dependencies are being set.
    - manifest: The manifest dictionary that includes the DAG structure.
    - model_op_dict: A dictionary mapping model nodes to their corresponding operators.
    - test_op_dict: A dictionary mapping test nodes to their corresponding operators.
    - models_with_crit_test_dependencies: A list of models that have critical tests attached.
    - final_op: The final operation (e.g., compilation) that should be triggered after all dependencies.
    """

    # Get the children of the current model node
    children = [
        model_op_dict[child]
        for child in manifest["child_map"].get(model_node, [])
        if child in model_op_dict
    ]

    # Check if the model has critical test dependencies
    if model_node in models_with_crit_test_dependencies:
        # Link the test operator to its model's children or final_op
        if children:
            test_op_dict[model_node] >> children
        else:
            test_op_dict[model_node] >> final_op
    else:
        # No critical test, link model directly to its children or final_op
        if children:
            model_op_dict[model_node] >> children
        else:
            model_op_dict[model_node] >> final_op

    # Ensure that if a model has no children and no critical tests, it triggers the final operation
    if not children and model_node not in models_with_crit_test_dependencies:
        model_op_dict[model_node] >> final_op


def create_snapshot_operator(
    snapshot_node: str, snapshot_data: Dict, dag: DAG, group_id: str = None
) -> BashOperator:
    return BashOperator(
        task_id=snapshot_data["alias"],
        bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_snapshot.sh ",
        env={
            "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
            "target": "{{ params.target }}",
            "snapshot": f"""{snapshot_data['name']}""",
            "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
        },
        append_env=True,
        cwd=PATH_TO_DBT_PROJECT,
        dag=dag,
    )


def create_snapshot_group(
    dag: DAG, dbt_snapshots: List[str], manifest: Dict
) -> Dict[str, BashOperator]:
    snapshot_op_dict: Dict[str, BashOperator] = {}

    for snapshot_node in dbt_snapshots:
        snapshot_data = manifest["nodes"][snapshot_node]
        snapshot_op_dict[snapshot_node] = create_snapshot_operator(
            snapshot_node, snapshot_data, dag
        )
    return snapshot_op_dict


def create_data_transformation_group(
    dag: DAG,
    dbt_models: List[str],
    manifest: Dict,
    models_with_crit_test_dependencies: List[Optional[str]],
    test_op_dict: Dict[str, BashOperator],
) -> Dict[str, BashOperator]:
    model_op_dict: Dict[str, BashOperator] = {}
    with TaskGroup(group_id="data_transformation", dag=dag) as data_transfo:
        with TaskGroup(group_id="applicative_tables", dag=dag) as applicative:
            for model_node in dbt_models:
                model_data = manifest["nodes"][model_node]
                if "applicative" in model_data["alias"]:
                    model_op_dict[model_node] = create_model_operator(
                        model_node, model_data, True, dag
                    )
                    if model_node in models_with_crit_test_dependencies:
                        model_op_dict[model_node] >> test_op_dict[model_node]

        for model_node in dbt_models:
            model_data = manifest["nodes"][model_node]
            if "applicative" not in model_data["alias"]:
                model_op_dict[model_node] = create_model_operator(
                    model_node, model_data, False, dag
                )
                if model_node in models_with_crit_test_dependencies:
                    model_op_dict[model_node] >> test_op_dict[model_node]
    return model_op_dict, data_transfo


# Main script for dbt DAG reconstruction
def dbt_dag_reconstruction(
    dag: DAG,
    manifest: Dict,
    dbt_models: List[str],
    dbt_snapshots: List[str],
    models_with_crit_test_dependencies: List[Optional[str]],
    crit_test_parents: Dict[Optional[str], List[str]],
    compile_op: DummyOperator,
) -> Dict[str, Union[Dict[str, BashOperator], TaskGroup]]:
    # Initialize the dictionaries for tasks
    test_op_dict: Dict[str, BashOperator] = {}
    model_op_dict: Dict[str, BashOperator] = {}
    snapshot_op_dict: Dict[str, BashOperator] = {}

    # Create the critical tests TaskGroup
    with TaskGroup(group_id="critical_tests", dag=dag) as crit_test_group:
        for model_node in dbt_models:
            model_data = manifest["nodes"][model_node]
            if model_node in models_with_crit_test_dependencies:
                test_op_dict[model_node] = create_test_operator(
                    model_node, model_data, dag
                )

    # Create the data transformation TaskGroup
    model_op_dict, data_transfo_group = create_data_transformation_group(
        dag, dbt_models, manifest, models_with_crit_test_dependencies, test_op_dict
    )

    # Create the snapshot TaskGroup
    with TaskGroup(group_id="snapshots", dag=dag) as snapshot_group:
        for snapshot_node in dbt_snapshots:
            snapshot_data = manifest["nodes"][snapshot_node]
            snapshot_op_dict[snapshot_node] = create_snapshot_operator(
                snapshot_node, snapshot_data, dag
            )

    # Manage test's cross dependencies
    for test, parents in crit_test_parents.items():
        for p in parents:
            try:
                model_op_dict[p] >> test_op_dict[test]
            except KeyError:
                pass

    # Return the created operators and TaskGroups
    return {
        "crit_test_group": crit_test_group,
        "data_transfo_group": data_transfo_group,
        "snapshot_group": snapshot_group,
        "test_op_dict": test_op_dict,
        "model_op_dict": model_op_dict,
        "snapshot_op_dict": snapshot_op_dict,
    }


def get_models_folder_dict(
    dbt_models: List[str], manifest: Optional[Dict]
) -> Dict[str, List[str]]:
    """Return a dictionary associating dbt nodes in "model" folder to the list of folders containing it.
    Example output:
    {model2_id:[folder_A,subfolder_A],model2_id:[folder_A,subfolder_B,subsubfolderB],model3_id:[folder_B]}
    """
    return {
        model_node: manifest["nodes"][model_node]["fqn"][1:-1]
        for model_node in dbt_models
    }


def create_nested_folder_groups(
    dbt_models: List[str],
    model_op_dict: Dict[str, BaseOperator],
    manifest: Optional[Dict],
    dag: DAG,
) -> None:
    nested_folders = get_models_folder_dict(dbt_models, manifest)
    task_groups: Dict[str, Tuple[TaskGroup, DummyOperator]] = {}

    def create_nested_task_group(
        folder_hierarchy: List[str],
        original_hierarchy: List[str],
        parent_group: Optional[TaskGroup] = None,
        current_path: str = "",
    ) -> Tuple[Optional[TaskGroup], List[str]]:
        """
        Recursively create nested TaskGroups based on folder hierarchy.
        This function modifies task_group dictionary declared above.
        TODO:
            - Remove leaf TaskGroup and put the trigger operator in parent TaskGroup.
            Hint: the recursive function is idempotent over original_hierarchy.
            - Internalize task_group dict and output it.
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
            # Create task group & corresponding trigger operator
            tg = TaskGroup(group_id=group_id, parent_group=parent_group, dag=dag)
            dummy_task = DummyOperator(
                task_id=f"trigger_{group_id}_folder", task_group=tg, dag=dag
            )
            # Add them to dictionary
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


def create_folder_manual_trigger_group(
    dag: DAG,
    dbt_models: List[str],
    manifest: Dict,
    model_op_dict: Dict[str, BaseOperator],
    group_name: str = "folders_manual_trigger",
) -> TaskGroup:
    with TaskGroup(group_id=group_name, dag=dag) as trigger_block:
        create_nested_folder_groups(dbt_models, model_op_dict, manifest, dag)
    return trigger_block
