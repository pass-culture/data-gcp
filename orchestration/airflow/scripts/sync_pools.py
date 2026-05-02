#!/usr/bin/env python3
import json
import os
from textwrap import indent

import typer
from airflow import settings
from airflow.models.pool import Pool

app = typer.Typer(help="Manage Airflow pools against a desired JSON configuration.")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
POOLS_JSON = os.path.join(SCRIPT_DIR, "pools.json")
PROTECTED_POOLS = ["default_pool"]


# -------------------------
# LOAD DESIRED CONFIG
# -------------------------
def load_desired_pools() -> dict:
    try:
        with open(POOLS_JSON, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
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

    # normalize structure
    normalized = {}
    for name, cfg in data.items():
        normalized[name] = {
            "slots": int(cfg.get("slots", 0)),
            "description": cfg.get("description", "") or "",
        }
    return normalized


# -------------------------
# LOAD CURRENT STATE (ORM)
# -------------------------
def load_current_pools() -> dict:
    session = settings.Session()
    try:
        pools = session.query(Pool).all()
        return {
            p.pool: {
                "slots": int(p.slots),
                "description": p.description or "",
            }
            for p in pools
        }
    finally:
        session.close()


# -------------------------
# DIFF LOGIC
# -------------------------
def diff_pools(expected: dict, actual: dict):
    exp, act = set(expected), set(actual)
    missing = exp - act
    extras = act - exp

    mismatches = []
    for name in exp & act:
        e, a = expected[name], actual[name]
        errs = []
        if e["slots"] != a["slots"]:
            errs.append(f"slots: expected {e['slots']}, got {a['slots']}")
        if e["description"] != a["description"]:
            errs.append(
                f"description: expected '{e['description']}', got '{a['description']}'"
            )
        if errs:
            mismatches.append((name, errs))

    return missing, extras, mismatches


# -------------------------
# VERIFY
# -------------------------
@app.command()
def verify():
    expected = load_desired_pools()
    actual = load_current_pools()

    missing, extras, mismatches = diff_pools(expected, actual)

    protected = sorted(p for p in extras if p in PROTECTED_POOLS)
    unprotected = sorted(p for p in extras if p not in PROTECTED_POOLS)

    if not missing and not unprotected and not mismatches:
        typer.secho(
            "✅ Pools exactly match expected configuration.",
            fg=typer.colors.GREEN,
        )
        if protected:
            typer.secho(
                "⚠️  Unmanaged protected pools present (OK):\n  "
                + "\n  ".join(protected),
                fg=typer.colors.YELLOW,
            )
        raise typer.Exit(code=0)

    if missing:
        typer.secho("❌ Missing pools:", fg=typer.colors.RED)
        typer.echo(indent("\n".join(sorted(missing)), "  "))

    if unprotected:
        typer.secho("❌ Extra pools present:", fg=typer.colors.RED)
        typer.echo(indent("\n".join(unprotected), "  "))

    if protected:
        typer.secho(
            "⚠️  Unmanaged protected pools (not an error):",
            fg=typer.colors.YELLOW,
        )
        typer.echo(indent("\n".join(protected), "  "))

    if mismatches:
        typer.secho("❌ Pools with attribute mismatches:", fg=typer.colors.RED)
        for name, errs in mismatches:
            typer.echo(f"  – {name}:")
            typer.echo(indent("\n".join(errs), "    "))

    raise typer.Exit(code=1)


# -------------------------
# CREATE / UPDATE
# -------------------------
@app.command()
def create():
    desired = load_desired_pools()
    session = settings.Session()

    try:
        for name, cfg in desired.items():
            slots = cfg["slots"]
            desc = cfg["description"]

            pool = session.query(Pool).filter(Pool.pool == name).one_or_none()

            if pool:
                updated = False

                if pool.slots != slots:
                    pool.slots = slots
                    updated = True

                if (pool.description or "") != desc:
                    pool.description = desc
                    updated = True

                if pool.include_deferred:
                    pool.include_deferred = False
                    updated = True

                if updated:
                    typer.echo(f"Updated pool {name}")
                else:
                    typer.echo(f"Unchanged pool {name}")

            else:
                session.add(
                    Pool(
                        pool=name,
                        slots=slots,
                        description=desc,
                        include_deferred=False,
                    )
                )
                typer.echo(f"Created pool {name}")

        session.commit()

    finally:
        session.close()

    raise typer.Exit(code=0)


# -------------------------
# SYNC
# -------------------------
@app.command()
def sync():
    try:
        verify()
    except typer.Exit as e:
        if e.exit_code == 1:
            typer.secho("🔧 Syncing pools...", fg=typer.colors.YELLOW)
            create()
        else:
            raise


if __name__ == "__main__":
    app()
