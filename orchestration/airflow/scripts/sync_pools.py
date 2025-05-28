#!/usr/bin/env python3
import json
import os
import subprocess
import sys
from textwrap import indent

import typer
from airflow import settings
from airflow.models.pool import Pool

app = typer.Typer(help="Manage Airflow pools against a desired JSON configuration.")

# Path to pools.json (assumes it's alongside this script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
POOLS_JSON = os.path.join(SCRIPT_DIR, "pools.json")


def load_desired_pools() -> dict:
    """
    Load desired pool definitions from pools.json.
    """
    with open(POOLS_JSON, "r") as f:
        data = json.load(f)
    # Validate structure
    for name, cfg in data.items():
        if "slots" not in cfg or "description" not in cfg:
            typer.secho(
                f"Config error: pool '{name}' missing 'slots' or 'description'",
                err=True,
                fg=typer.colors.RED,
            )
            raise typer.Exit(code=2)
    return data


def load_current_pools() -> dict:
    """
    Fetch current pools via the Airflow CLI (JSON output).
    """
    raw = subprocess.check_output(
        ["airflow", "pools", "list", "--output", "json"], stderr=subprocess.STDOUT
    )
    lst = json.loads(raw)
    return {
        p["name"]: {"slots": p["slots"], "description": p.get("description", "")}
        for p in lst
    }


def diff_pools(expected: dict, actual: dict):
    """
    Compare expected vs. actual: return (missing, extra, mismatches).
    """
    exp_keys = set(expected)
    act_keys = set(actual)
    missing = exp_keys - act_keys
    extra = act_keys - exp_keys
    mismatches = []

    for name in exp_keys & act_keys:
        e, a = expected[name], actual[name]
        errs = []
        if e["slots"] != a["slots"]:
            errs.append(f" slots: expected {e['slots']}, got {a['slots']}")
        if e.get("description", "") != a.get("description", ""):
            errs.append(
                f" description: expected '{e['description']}', got '{a['description']}'"
            )
        if errs:
            mismatches.append((name, errs))
    return missing, extra, mismatches


@app.command()
def verify():
    """
    Verify that current Airflow pools match pools.json.
    """
    expected = load_desired_pools()
    actual = load_current_pools()
    missing, extra, mismatches = diff_pools(expected, actual)

    if not missing and not extra and not mismatches:
        typer.secho(
            "✅ Pools exactly match expected configuration.", fg=typer.colors.GREEN
        )
        raise typer.Exit(code=0)

    if missing:
        typer.secho("❌ Missing pools:", fg=typer.colors.RED)
        typer.echo(indent("\n".join(sorted(missing)), "  "))
    if extra:
        typer.secho("❌ Extra pools present:", fg=typer.colors.RED)
        typer.echo(indent("\n".join(sorted(extra)), "  "))
    if mismatches:
        typer.secho("❌ Pools with attribute mismatches:", fg=typer.colors.RED)
        for name, errs in mismatches:
            typer.echo(f"  – {name}:")
            typer.echo(indent("\n".join(errs), "    "))

    raise typer.Exit(code=1)


@app.command()
def create():
    """
    Create or update Airflow pools based on pools.json.
    """
    desired = load_desired_pools()
    session = settings.Session()
    for name, cfg in desired.items():
        slots = cfg.get("slots", 0)
        desc = cfg.get("description", "")
        pool_obj = session.query(Pool).filter(Pool.pool == name).one_or_none()
        if pool_obj:
            pool_obj.slots = slots
            pool_obj.description = desc
            pool_obj.include_deferred = False
            typer.echo(f"Updated pool {name} → slots={slots}, desc='{desc}'")
        else:
            session.add(
                Pool(pool=name, slots=slots, description=desc, include_deferred=False)
            )
            typer.echo(f"Created pool {name} → slots={slots}, desc='{desc}'")
    session.commit()
    session.close()
    raise typer.Exit(code=0)


@app.command()
def sync():
    """
    Verify pools, and if out-of-sync, create/update them.
    """
    try:
        verify()
    except typer.Exit as e:
        if e.exit_code != 0:
            create()
        else:
            raise


if __name__ == "__main__":
    app()
