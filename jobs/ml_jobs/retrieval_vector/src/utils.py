import json


def save_model_type(model_type: dict, output_dir: str):
    """
    Save model type configuration to JSON file.

    Args:
        model_type (dict): Dictionary containing model type configuration
        output_dir (str): Directory path where the JSON file will be saved
    """
    with open(f"{output_dir}/model_type.json", "w") as file:
        json.dump(model_type, file)
