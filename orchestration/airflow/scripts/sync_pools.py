#!/usr/bin/env python3
import json
import os
import subprocess
from textwrap import indent

import typer
from airflow import settings
from airflow.models.pool import Pool

app = typer.Typer(help="Manage Airflow pools against a desired JSON configuration.")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
POOLS_JSON = os.path.join(SCRIPT_DIR, "pools.json")
PROTECTED_POOLS = ["default_pool"]


def load_desired_pools() -> dict:
    try:
        with open(POOLS_JSON, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        # Warn and exit “success”—we’ll just rely on default_pool
        typer.secho(
            f"⚠️  '{POOLS_JSON}' not found; using only the built-in default_pool.",
            fg=typer.colors.YELLOW,
        )
        raise typer.Exit(code=0)
    except json.JSONDecodeError as e:
        typer.secho(
            f"Error parsing '{POOLS_JSON}': {e}",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)
    return data


def load_current_pools() -> dict:
    raw = subprocess.check_output(
        ["airflow", "pools", "list", "--output", "json"], stderr=subprocess.STDOUT
    )
    lst = json.loads(raw)
    return {
        p["pool"]: {
            "slots": int(p["slots"]),
            "description": p.get("description", "") or "",
        }
        for p in lst
    }


def diff_pools(expected: dict, actual: dict):
    exp, act = set(expected), set(actual)
    missing = exp - act
    extras = act - exp
    mismatches = []
    for name in exp & act:
        e, a = expected[name], actual[name]
        errs = []
        if e["slots"] != a["slots"]:
            errs.append(f" slots: expected {e['slots']}, got {a['slots']}")
        if e["description"] != a["description"]:
            errs.append(
                f" description: expected '{e['description']}', got '{a['description']}'"
            )
        if errs:
            mismatches.append((name, errs))
    return missing, extras, mismatches


@app.command()
def verify():
    """
    Verify that current Airflow pools match pools.json,
    but never treat PROTECTED_POOLS as a failure.
    """
    expected = load_desired_pools()
    actual = load_current_pools()

    missing, extras, mismatches = diff_pools(expected, actual)
    protected = sorted(p for p in extras if p in PROTECTED_POOLS)
    unprotected = sorted(p for p in extras if p not in PROTECTED_POOLS)

    # 1) If nothing to fix (no missing, no unprotected extras, no mismatches) → SUCCESS
    if not missing and not unprotected and not mismatches:
        typer.secho(
            "✅ Pools exactly match expected configuration.", fg=typer.colors.GREEN
        )
        if protected:
            typer.secho(
                "⚠️  Unmanaged protected pools present (OK):\n  "
                + "\n  ".join(protected),
                fg=typer.colors.YELLOW,
            )
        raise typer.Exit(code=0)

    # 2) Otherwise, report problems
    if missing:
        typer.secho("❌ Missing pools:", fg=typer.colors.RED)
        typer.echo(indent("\n".join(sorted(missing)), "  "))
    if unprotected:
        typer.secho("❌ Extra pools present:", fg=typer.colors.RED)
        typer.echo(indent("\n".join(unprotected), "  "))
    if protected:
        typer.secho(
            "⚠️  Unmanaged protected pools (not an error):", fg=typer.colors.YELLOW
        )
        typer.echo(indent("\n".join(protected), "  "))
    if mismatches:
        typer.secho("❌ Pools with attribute mismatches:", fg=typer.colors.RED)
        for name, errs in mismatches:
            typer.echo(f"  – {name}:")
            typer.echo(indent("\n".join(errs), "    "))

    # 3) Fail only if there are missing, unprotected extras, or mismatches
    raise typer.Exit(code=1)


@app.command()
def create():
    """
    Create or update Airflow pools based on pools.json.
    """
    desired = load_desired_pools()
    session = settings.Session()
    for name, cfg in desired.items():
        slots, desc = cfg["slots"], cfg["description"]
        pool = session.query(Pool).filter(Pool.pool == name).one_or_none()
        if pool:
            pool.slots, pool.description, pool.include_deferred = slots, desc, False
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
    Verify pools, and if *out-of-sync* (exit code 1), create/update them.
    """
    try:
        verify()
    except typer.Exit as e:
        # Only trigger creation on the “out-of-sync” exit code
        if e.exit_code == 1:
            create()
        else:
            # e.exit_code == 0 → success, e.exit_code == 2 → bad config
            raise


if __name__ == "__main__":
    app()
