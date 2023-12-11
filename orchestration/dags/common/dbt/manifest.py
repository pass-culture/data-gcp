import json


def load_manifest(_PATH_TO_DBT_TARGET):
    local_filepath = _PATH_TO_DBT_TARGET + "/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data


def build_simplified_manifest(data):
    simplified_manifest = {
        node: {
            "redirect_dep": None,
            "model_alias": data["nodes"][node]["alias"],
            "depends_on_node": data["nodes"][node]["depends_on"]["nodes"],
            "model_tests": {},
            "resource_type": data["nodes"][node]["resource_type"],
        }
        for node in data["nodes"].keys()
        if (
            data["nodes"][node]["resource_type"] == "model" and "elementary" not in node
        )
    }
    for node in data["nodes"].keys():
        if data["nodes"][node]["resource_type"] == "test":
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
                data["nodes"][node]["alias"]
                if not generic_test
                else node.split(".")[-2]
            )
            test_config = data["nodes"][node]["config"].get("severity", None)
            try:
                test_config = test_config.lower()
            except AttributeError:
                pass
            parents = data["nodes"][node]["depends_on"]["nodes"]
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


def rebuild_manifest(_PATH_TO_DBT_PROJECT):
    try:
        data = load_manifest(_PATH_TO_DBT_PROJECT)
        simplified_manifest = build_simplified_manifest(data)
    except FileNotFoundError:
        simplified_manifest = {}
    return simplified_manifest
