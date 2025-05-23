#!/usr/bin/env python3
import json
import os
import subprocess
import sys
from textwrap import indent

script_dir = os.path.dirname(os.path.abspath(__file__))
EXPECTED_PATH = os.path.abspath(os.path.join(script_dir, "pools.json"))


def load_expected(path):
    with open(path, "r") as f:
        data = json.load(f)
    # Ensure every value has both keys
    for name, cfg in data.items():
        if "slots" not in cfg or "description" not in cfg:
            print(
                f"Config error in expected JSON: pool '{name}' missing 'slots' or 'description'"
            )
            sys.exit(2)
    return data


def load_actual():
    raw = subprocess.check_output(
        ["airflow", "pools", "list", "--output", "json"], stderr=subprocess.STDOUT
    )
    lst = json.loads(raw)
    return {
        p["name"]: {"slots": p["slots"], "description": p.get("description", "")}
        for p in lst
    }


def diff(expected, actual):
    exp_keys = set(expected)
    act_keys = set(actual)

    missing = exp_keys - act_keys
    extra = act_keys - exp_keys
    mismatches = []

    for name in exp_keys & act_keys:
        e = expected[name]
        a = actual[name]
        errs = []
        if e["slots"] != a["slots"]:
            errs.append(f" slots: expected {e['slots']}, got {a['slots']}")
        if (e.get("description", "") or "") != (a.get("description", "") or ""):
            errs.append(
                f" description: expected '{e['description']}', got '{a['description']}'"
            )
        if errs:
            mismatches.append((name, errs))

    return missing, extra, mismatches


def main():
    expected = load_expected(EXPECTED_PATH)
    actual = load_actual()
    missing, extra, mismatches = diff(expected, actual)

    if not missing and not extra and not mismatches:
        print("✅ Pools exactly match expected configuration.")
        sys.exit(0)

    if missing:
        print("❌ Missing pools:")
        print(indent("\n".join(sorted(missing)), "  "))
    if extra:
        print("❌ Extra pools present:")
        print(indent("\n".join(sorted(extra)), "  "))
    if mismatches:
        print("❌ Pools with attribute mismatches:")
        for name, errs in mismatches:
            print(f"  – {name}:")
            print(indent("\n".join(errs), "    "))

    sys.exit(1)


if __name__ == "__main__":
    main()
