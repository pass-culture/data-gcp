import json


def load_json_artifact(_PATH_TO_DBT_TARGET, artifact):
    local_filepath = _PATH_TO_DBT_TARGET + "/" + artifact
    with open(local_filepath) as f:
        data = json.load(f)
    return data


def load_manifest(_PATH_TO_DBT_TARGET):
    return load_json_artifact(_PATH_TO_DBT_TARGET, "manifest.json")


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

    simplified_manifest = simplified_manifest_models | simplified_manifest_sources

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
