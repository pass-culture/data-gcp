import json
import os

from functools import partial
from typing import Optional, Union, Tuple
from pathlib import Path
from functools import lru_cache

from common.config import EXCLUDED_TAGS

from airflow import DAG
from airflow.models.baseoperator import BaseOperator, chain
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Import the Python callable functions from dbt_executors module
from common.dbt.dbt_executors import run_dbt_model, run_dbt_test, run_dbt_snapshot


def get_models_folder_dict(
    dbt_models: list[str], manifest: Optional[dict]
) -> dict[str, list[str]]:
    """Return a dictionary associating dbt nodes in "model" folder to the list of folders containing it.
    Example output:
    {model2_id:[folder_A,subfolder_A],model2_id:[folder_A,subfolder_B,subsubfolderB],model3_id:[folder_B]}
    """
    return {
        model_node: manifest["nodes"][model_node]["fqn"][1:-1]
        for model_node in dbt_models
    }


def create_nested_folder_groups(
    dbt_models: list[str],
    model_op_dict: dict[str, BaseOperator],
    manifest: Optional[dict],
    dag: DAG,
) -> None:
    nested_folders = get_models_folder_dict(dbt_models, manifest)
    task_groups: dict[str, Tuple[TaskGroup, EmptyOperator]] = {}

    def create_nested_task_group(
        folder_hierarchy: list[str],
        original_hierarchy: list[str],
        parent_group: Optional[TaskGroup] = None,
        current_path: str = "",
    ) -> Tuple[Optional[TaskGroup], list[str]]:
        """
        Recursively create nested TaskGroups based on folder hierarchy.
        This function modifies task_group dictionary declared above.
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
            dummy_task = EmptyOperator(
                task_id=f"trigger_{group_id}_folder", task_group=tg, dag=dag, pool="dbt"
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


@lru_cache(maxsize=128)
def load_json_artifact_with_mtime(
    path_to_dbt_target: str, artifact: str, mtime: float
) -> dict:
    """
    Load and cache a JSON artifact with mtime as cache key.
    Cache invalidates automatically when artifact file changes.

    Args:
        path_to_dbt_target: Path to dbt target directory
        artifact: Artifact filename (e.g., "manifest.json", "run_results.json")
        mtime: File modification time (part of cache key)

    Returns:
        Parsed JSON dictionary
    """
    if mtime == 0.0:
        return {}
    local_filepath = os.path.join(path_to_dbt_target, artifact)
    try:
        with open(local_filepath) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def load_json_artifact(path_to_dbt_target: str, artifact: str) -> dict:
    """
    Load JSON artifact with automatic cache invalidation on file changes.
    Wrapper that handles mtime checking.
    """
    local_filepath = os.path.join(path_to_dbt_target, artifact)
    try:
        mtime = os.path.getmtime(local_filepath)
    except FileNotFoundError:
        mtime = 0.0

    return load_json_artifact_with_mtime(path_to_dbt_target, artifact, mtime)


@lru_cache(maxsize=128)
def load_manifest_with_mtime(path_to_dbt_target: str, mtime: float) -> dict:
    """
    Load and cache manifest with mtime as cache key.
    Cache invalidates automatically when manifest changes.

    Args:
        path_to_dbt_target: Path to dbt target directory
        mtime: File modification time (0.0 = file doesn't exist)

    Returns:
        Parsed manifest dict, or empty manifest structure if file doesn't exist
    """
    # Special case: return empty manifest structure if file doesn't exist
    if mtime == 0.0:
        return {
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

    # Load the actual manifest
    manifest = load_json_artifact_with_mtime(path_to_dbt_target, "manifest.json", mtime)

    # If loaded manifest is empty (shouldn't happen since mtime != 0.0), return structure
    if not manifest:
        return {
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

    return manifest


def load_manifest(path_to_dbt_target: str) -> dict:
    """
    Load manifest with automatic cache invalidation on file changes.
    Returns empty manifest structure if file doesn't exist (e.g., in CI).
    """
    manifest_file = os.path.join(path_to_dbt_target, "manifest.json")
    try:
        mtime = os.path.getmtime(manifest_file)
    except FileNotFoundError:
        mtime = 0.0

    return load_manifest_with_mtime(path_to_dbt_target, mtime)


def get_node_data(node: str, manifest: dict, is_full_node_id: bool = True) -> dict:
    """
    Get node data from manifest.

    Args:
        node: Either full node ID (e.g., "model.data_gcp_dbt.my_model")
              or just the name (e.g., "my_model")
        manifest: The loaded dbt manifest
        is_full_node_id: Whether node is a full ID or just the name

    Returns:
        Node data dictionary
    """
    nodes = manifest.get("nodes", {})

    if is_full_node_id:
        if node not in nodes:
            raise KeyError(f"Node {node} not found in manifest")
        return nodes[node]

    return nodes.get(node, {})


def get_node_property_with_config_precedence(
    node_data: dict,
    property_name: str,
    default: any = None,
    merge_behavior: str = "replace",
) -> any:
    """
    Get a property from node data with config precedence.

    Args:
        node_data: The node data dictionary
        property_name: Property to retrieve (e.g., "alias", "schema", "tags")
        default: Default value if property not found
        merge_behavior: "replace" (default) or "merge" for list/dict properties

    Returns:
        The property value respecting config precedence
    """
    config_value = node_data.get("config", {}).get(property_name)
    base_value = node_data.get(property_name, default)

    # Special handling for properties with merge behavior
    if merge_behavior == "merge":
        if property_name == "tags":
            # Tags are additive - combine both lists
            base_tags = base_value if isinstance(base_value, list) else []
            config_tags = config_value if isinstance(config_value, list) else []
            return list(set(base_tags + config_tags))
        elif property_name == "meta":
            # Meta dictionaries are merged
            base_meta = base_value if isinstance(base_value, dict) else {}
            config_meta = config_value if isinstance(config_value, dict) else {}
            return {**base_meta, **config_meta}

    # Default behavior: config takes precedence only if it's not None
    return config_value if config_value is not None else base_value


def get_node_alias_and_dataset(
    node_name: str, manifest: dict, node_type: str = "model"
) -> Tuple[str, str]:
    """
    Get the alias and dataset (schema) for a node from the manifest.

    Manifest contains resolved values regardless of config source:
    - config() block in .sql file (highest precedence)
    - properties in .yml file (medium precedence)
    - dbt_project.yml (lowest precedence)
    - defaults (filename for alias, target schema for schema)

    dbt has already done all the work - just read the final values.
    """
    node_prefix = f"{node_type}.data_gcp_dbt"
    full_node_id = f"{node_prefix}.{node_name}"

    nodes = manifest.get("nodes", {})
    if full_node_id not in nodes:
        raise KeyError(f"Node {full_node_id} not found in manifest")

    node_data = nodes[full_node_id]

    # Always use root-level fields - they're the resolved truth
    alias = node_data.get("alias")
    dataset = node_data.get("schema")

    return alias, dataset


def get_node_tags(node: str, node_manifest: dict) -> list[str]:
    """
    Get all tags for a node (base + config tags combined).
    Tags are additive in dbt.
    """
    node_data = node_manifest.get(node, {})

    base_tags = node_data.get("tags", [])
    config_tags = node_data.get("config", {}).get("tags", [])

    return list(set(base_tags + config_tags))


def get_node_config(node: str, node_manifest: dict) -> dict:
    """Get the config dictionary for a node."""
    node_data = node_manifest.get(node, {})
    return node_data.get("config", {})


def get_node_labels(node: str, node_manifest: dict) -> dict:
    """Get the labels dictionary from a node's config."""
    node_data = node_manifest.get(node, {})
    return get_node_property_with_config_precedence(node_data, "labels", default={})


@lru_cache(maxsize=128)
def get_models_schedule_from_manifest_cached(
    nodes_tuple: Tuple[str, ...],
    path_to_dbt_target: str,
    manifest_mtime: float,
    allowed_schedule_tuple: Tuple[str, ...] = ("daily", "weekly", "monthly"),
    resource_type: str = "model",
) -> dict:
    """
    Cached version of get_models_schedule_from_manifest.
    Cache invalidates when manifest changes.
    """
    nodes_manifest = load_manifest_with_mtime(path_to_dbt_target, manifest_mtime).get(
        "nodes", {}
    )
    node_prefix = f"{resource_type}.data_gcp_dbt"
    allowed_schedule = list(allowed_schedule_tuple)

    schedules = {}
    for n in nodes_tuple:
        tags = get_node_tags(f"{node_prefix}.{n}", nodes_manifest)
        labels = get_node_labels(f"{node_prefix}.{n}", nodes_manifest)

        schedules[n] = {
            "schedule_config": [tag for tag in tags if tag in allowed_schedule]
            + (
                [labels.get("schedule")]
                if (sched := labels.get("schedule")) in allowed_schedule
                and sched not in tags
                else []
            )
        }

        if len(schedules[n]["schedule_config"]) > 1:
            raise ValueError(f"Node {n} has multiple schedules defined: {schedules[n]}")
    return schedules


def get_models_schedule_from_manifest(
    nodes: Union[str, list[str]],
    path_to_dbt_target: Path,
    allowed_schedule: list[str] = None,
    resource_type: str = "model",
) -> dict:
    """
    Get schedule configuration for nodes from manifest.
    Automatically caches and invalidates on manifest changes.
    """
    if allowed_schedule is None:
        allowed_schedule = ["daily", "weekly", "monthly"]

    manifest_file = os.path.join(str(path_to_dbt_target), "manifest.json")
    try:
        mtime = os.path.getmtime(manifest_file)
    except FileNotFoundError:
        return {}

    if isinstance(nodes, str):
        nodes = [nodes]

    return get_models_schedule_from_manifest_cached(
        nodes_tuple=tuple(nodes),
        path_to_dbt_target=str(path_to_dbt_target),
        manifest_mtime=mtime,
        allowed_schedule_tuple=tuple(allowed_schedule),
        resource_type=resource_type,
    )


@lru_cache(maxsize=128)
def build_simplified_manifest_cached(
    path_to_dbt_target: str, manifest_mtime: float
) -> dict[str, dict[str, Union[str, None, list[str], dict[str, list[dict[str, str]]]]]]:
    """
    Build simplified manifest with caching.
    Cache invalidates when manifest changes.
    """
    json_dict_data = load_manifest_with_mtime(path_to_dbt_target, manifest_mtime)

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


def build_simplified_manifest(json_dict_data: dict) -> dict:
    """
    Build simplified manifest from raw manifest data.
    Note: This function is kept for backwards compatibility.
    For better performance, use rebuild_manifest() which includes caching.
    """
    # Extract path and mtime for caching if we have a full manifest
    # Otherwise, just process the data directly
    return build_simplified_manifest_cached.__wrapped__(
        path_to_dbt_target="", manifest_mtime=0.0
    )


def rebuild_manifest(path_to_dbt_target: str) -> dict:
    """
    Rebuild simplified manifest with automatic caching.
    Cache invalidates when manifest file changes.
    """
    manifest_file = os.path.join(path_to_dbt_target, "manifest.json")
    try:
        mtime = os.path.getmtime(manifest_file)
        return build_simplified_manifest_cached(path_to_dbt_target, mtime)
    except FileNotFoundError:
        return {}


@lru_cache(maxsize=128)
def load_run_results_with_mtime(
    path_to_dbt_target: str, mtime: float
) -> dict[str, dict]:
    """
    Load and cache run_results.json with mtime as cache key.
    """
    if mtime == 0.0:
        return {}

    json_dict_data = load_json_artifact_with_mtime(
        path_to_dbt_target, "run_results.json", mtime
    )
    dict_results = {}
    for item in json_dict_data.get("results", []):
        dict_results[item["unique_id"]] = {
            **dict_results.get(item["unique_id"], {}),
            **item,
        }
    return dict_results


def load_run_results(path_to_dbt_target: str) -> dict[str, dict]:
    """
    Load run results with automatic cache invalidation on file changes.
    Returns empty dict if file doesn't exist (e.g., in CI).
    """
    run_results_file = os.path.join(path_to_dbt_target, "run_results.json")
    try:
        mtime = os.path.getmtime(run_results_file)
    except FileNotFoundError:
        # Use mtime=0.0 to signal file doesn't exist
        mtime = 0.0

    return load_run_results_with_mtime(path_to_dbt_target, mtime)


def load_and_process_manifest(
    manifest_path: str,
) -> Tuple[
    dict,
    list[str],
    list[str],
    list[str],
    list[str],
    list[Optional[str]],
    dict[Optional[str], list[str]],
]:
    """
    Load and process manifest with automatic caching.
    Cache invalidates when manifest changes.
    """
    manifest = load_manifest(manifest_path)
    dbt_snapshots: list[str] = []
    dbt_models: list[str] = []
    dbt_crit_tests: list[str] = []

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

    models_with_dependencies: list[str] = [
        node for node in manifest["child_map"].keys() if node in dbt_models
    ]

    models_with_crit_test_dependencies: list[Optional[str]] = [
        manifest["nodes"][node].get("attached_node") for node in dbt_crit_tests
    ]

    crit_test_parents: dict[Optional[str], list[str]] = {
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


def create_test_operator(model_node: str, model_data: dict, dag: DAG) -> PythonOperator:
    """Create a Python operator for running dbt tests."""
    node_tags = model_data.get("tags", [])
    return PythonOperator(
        task_id=model_data["alias"] + "_tests",
        python_callable=partial(run_dbt_test, model_data["name"], node_tags),
        dag=dag,
        pool="dbt",
        trigger_rule="none_failed_min_one_success",  # Run if upstream tasks succeed or skip
    )


def create_model_operator(
    model_node: str, model_data: dict, is_applicative: bool, dag: DAG
) -> PythonOperator:
    """Create a Python operator for running a dbt model."""
    # Filter out weekly/monthly from excluded tags since we handle them differently now
    exclude_tags = (
        [tag for tag in EXCLUDED_TAGS if tag not in ["weekly", "monthly"]]
        if EXCLUDED_TAGS
        else None
    )
    node_tags = model_data.get("tags", [])

    return PythonOperator(
        task_id=model_data["alias"] if is_applicative else model_data["name"],
        python_callable=partial(
            run_dbt_model, model_data["name"], exclude_tags, node_tags
        ),
        dag=dag,
        pool="dbt",
        trigger_rule="none_failed_min_one_success",  # Run if upstream tasks succeed or skip
    )


def create_snapshot_operator(
    snapshot_node: str, snapshot_data: dict, dag: DAG
) -> PythonOperator:
    """Create a Python operator for running a dbt snapshot."""
    from common.utils import delayed_waiting_operator

    node_tags = snapshot_data.get("tags", [])
    node_meta = snapshot_data["meta"]

    snapshot_op = PythonOperator(
        task_id=snapshot_data["alias"],
        python_callable=partial(run_dbt_snapshot, snapshot_data["name"], node_tags),
        dag=dag,
        pool="dbt",
        trigger_rule="none_failed_min_one_success",  # Run if upstream tasks succeed or skip
    )
    if (
        "external_dependency" in node_tags
        and "external_dag_id" in node_meta
        and "external_task_id" in node_meta
    ):
        sensor = delayed_waiting_operator(
            dag,
            external_dag_id=node_meta["external_dag_id"],
            external_task_id=node_meta["external_task_id"],
        )
        sensor >> snapshot_op

    return snapshot_op


def create_critical_tests_group(
    dag: DAG,
    dbt_models: list[str],
    manifest: dict,
    models_with_crit_test_dependencies: list[Optional[str]],
) -> dict[str, PythonOperator]:
    test_op_dict: dict[str, PythonOperator] = {}
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


def create_data_transformation_group(
    dag: DAG,
    dbt_models: list[str],
    manifest: dict,
    models_with_crit_test_dependencies: list[Optional[str]],
    test_op_dict: dict[str, PythonOperator],
) -> dict[str, PythonOperator]:
    model_op_dict: dict[str, PythonOperator] = {}
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
    return model_op_dict


def setup_dependencies(
    model_node: str,
    manifest: dict,
    model_op_dict: dict[str, BaseOperator],
    test_op_dict: dict[str, BaseOperator],
    snapshot_op_dict: dict[str, BaseOperator],
    models_with_crit_test_dependencies: list[Optional[str]],
    final_op: BaseOperator,
) -> None:
    children = [
        model_op_dict[child]
        for child in manifest["child_map"][model_node]
        if child in model_op_dict
    ] + [
        snapshot_op_dict[child]
        for child in manifest["child_map"][model_node]
        if child in snapshot_op_dict
    ]
    if model_node in snapshot_op_dict:
        snapshot_op_dict[model_node] >> (children if children else final_op)
    elif model_node in models_with_crit_test_dependencies:
        test_op_dict[model_node] >> (children if children else final_op)
    else:
        model_op_dict[model_node] >> (children if children else final_op)


def set_up_nodes_dependencies(
    dag: DAG,
    dbt_models: list[str],
    dbt_snapshots: list[str],
    manifest: dict,
    model_op_dict: dict[str, BaseOperator],
    test_op_dict: dict[str, BaseOperator],
    snapshot_op_dict: dict[str, BaseOperator],
    models_with_crit_test_dependencies: list[Optional[str]],
    final_op: Union[BaseOperator, PythonOperator],
) -> None:
    for model_node in dbt_models:
        setup_dependencies(
            model_node,
            manifest,
            model_op_dict,
            test_op_dict,
            snapshot_op_dict,
            models_with_crit_test_dependencies,
            final_op,
        )
    # Set up dependencies between models and snapshots
    for snapshot_node in dbt_snapshots:
        setup_dependencies(
            snapshot_node,
            manifest,
            model_op_dict,
            test_op_dict,
            snapshot_op_dict,
            models_with_crit_test_dependencies,
            final_op,
        )


def create_snapshot_group(
    dag: DAG, dbt_snapshots: list[str], manifest: dict
) -> dict[str, PythonOperator]:
    snapshot_op_dict: dict[str, PythonOperator] = {}
    with TaskGroup(group_id="snapshots", dag=dag) as snapshot_group:
        for snapshot_node in dbt_snapshots:
            snapshot_data = manifest["nodes"][snapshot_node]
            snapshot_op_dict[snapshot_node] = create_snapshot_operator(
                snapshot_node, snapshot_data, dag
            )
    return snapshot_op_dict


def create_folder_manual_trigger_group(
    dag: DAG,
    dbt_models: list[str],
    manifest: dict,
    model_op_dict: dict[str, BaseOperator],
) -> TaskGroup:
    with TaskGroup(group_id="folders_manual_trigger", dag=dag) as trigger_block:
        create_nested_folder_groups(dbt_models, model_op_dict, manifest, dag)
    return trigger_block


# Main script for dbt DAG reconstruction
def dbt_dag_reconstruction(
    dag: DAG,
    manifest: dict,
    dbt_models: list[str],
    dbt_snapshots: list[str],
    models_with_crit_test_dependencies: list[Optional[str]],
    crit_test_parents: dict[Optional[str], list[str]],
    final_op: Union[BaseOperator, PythonOperator],
) -> dict[str, Union[dict[str, PythonOperator], TaskGroup]]:
    # Create the task group for critical tests only if necessary
    test_op_dict = create_critical_tests_group(
        dag, dbt_models, manifest, models_with_crit_test_dependencies
    )

    # Create the task group for data transformation
    model_op_dict = create_data_transformation_group(
        dag, dbt_models, manifest, models_with_crit_test_dependencies, test_op_dict
    )
    # Create the snapshot group
    snapshot_op_dict = create_snapshot_group(dag, dbt_snapshots, manifest)

    # Set up dependencies between models, snapshots and tests
    set_up_nodes_dependencies(
        dag,
        dbt_models,
        dbt_snapshots,
        manifest,
        model_op_dict,
        test_op_dict,
        snapshot_op_dict,
        models_with_crit_test_dependencies,
        final_op,
    )

    # Manage test's cross dependencies
    for test, parents in crit_test_parents.items():
        for p in parents:
            try:
                model_op_dict[p] >> test_op_dict[test]
            except KeyError:
                pass

    # Create the folder manual trigger group
    trigger_block = create_folder_manual_trigger_group(
        dag, dbt_models, manifest, model_op_dict
    )

    # Return the created operators
    return {
        "test_op_dict": test_op_dict,
        "model_op_dict": model_op_dict,
        "snapshot_op_dict": snapshot_op_dict,
        "trigger_block": trigger_block,
    }
