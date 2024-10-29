import json


def read_json(filename="manifest.json"):
    with open(filename, "r") as f:
        data = json.load(f)
    return data


def write_json(data, filename="manifest.json"):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)


if __name__ == "__main__":
    data = read_json("target/manifest.json")
    for macro in data.get("macros", {}):
        data["macros"][macro]["docs"]["show"] = False
    write_json(data, "target/manifest.json")
