#!/usr/bin/env python3
import json
import os

from airflow import settings
from airflow.models.pool import Pool


def load_desired_pools():
    # locate pools.json next to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.abspath(os.path.join(script_dir, "pools.json"))
    with open(json_path, "r") as f:
        data = json.load(f)
    # expect a dict of { pool_name: { slots: int, description: str } }
    return data


def main():
    desired = load_desired_pools()
    session = settings.Session()

    for name, cfg in desired.items():
        slots = cfg.get("slots", 0)
        desc = cfg.get("description", "")
        p = session.query(Pool).filter(Pool.pool == name).one_or_none()
        if p:
            p.slots = slots
            p.description = desc
            p.include_deferred = False
            print(f"Updated pool {name} → slots={slots}, desc='{desc}'")
        else:
            session.add(
                Pool(pool=name, slots=slots, description=desc, include_deferred=False)
            )
            print(f"Created pool {name} → slots={slots}, desc='{desc}'")

    session.commit()
    session.close()


if __name__ == "__main__":
    main()
