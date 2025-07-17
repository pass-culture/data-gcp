#!/usr/bin/env uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ['dotenv', 'typer', 'google-cloud-storage']
# ///

# # uv python interpreters compiled with LibreSSL -> Ignore warnings related to urllib3 and OpenSSL
# # Note: This workaround fails as the warning is thrown by the gsutil call packing its own urllib3 library.
# import warnings
# from urllib3.exceptions import NotOpenSSLWarning
# warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated
from dotenv import load_dotenv

app = typer.Typer()


def find_dotenv():
    """Recherche récursive du fichier .env.dbt"""
    current_dir = os.getcwd()
    while current_dir != "/":
        potential = os.path.join(current_dir, ".env.dbt")
        if os.path.isfile(potential):
            return potential
        current_dir = os.path.dirname(current_dir)
    raise FileNotFoundError(".env.dbt file not found")


def pull_artifacts(from_env: str):
    bucket = set_bucket_name(from_env)
    if not bucket:
        typer.echo(f"Unknown environment: {from_env}")
        raise typer.Exit(code=1)

    artifacts_path = f"gs://{bucket}/data/target"
    local_dir = f"env-run-artifacts/{from_env}"
    os.makedirs(local_dir, exist_ok=True)

    for file in ["manifest.json", "run_results.json"]:
        subprocess.run(
            ["gsutil", "cp", f"{artifacts_path}/{file}", f"{local_dir}/{file}"],
            check=True,
        )
        print(f"Copied {file} to {local_dir}")


def compile_project(defer_to: str):
    compile_target_path = f"target/defer_to/{defer_to}"
    typer.echo(f"Recompiling project in : {compile_target_path}")

    subprocess.run(
        [
            "dbt",
            "compile",
            "--target",
            defer_to,
            "--target-path",
            compile_target_path,
            "--vars",
            f"{{'ENV_SHORT_NAME': '{defer_to}'}}",
        ],
        check=True,
    )


def set_bucket_name(defer_to: str):
    ENV_BUCKETS = {
        "dev": os.getenv("AIRFLOW_BUCKET_DEV"),
        "stg": os.getenv("AIRFLOW_BUCKET_STG"),
        "prod": os.getenv("AIRFLOW_BUCKET_PROD"),
    }
    return ENV_BUCKETS.get(defer_to, None)


def set_connection_id(defer_to: str, target_env: str):
    conn_ids = {
        "local": os.environ.get("APPLICATIVE_EXTERNAL_CONNECTION_ID_DEV"),
        "dev": os.environ.get("APPLICATIVE_EXTERNAL_CONNECTION_ID_DEV"),
        "stg": os.environ.get("APPLICATIVE_EXTERNAL_CONNECTION_ID_STG"),
        "prod": os.environ.get("APPLICATIVE_EXTERNAL_CONNECTION_ID_PROD"),
    }

    connection_id = None

    if defer_to:
        if defer_to in conn_ids.keys():
            connection_id = conn_ids[defer_to]
        else:
            print(f"❌ Unknown DEFER_LOCAL_RUN_TO environment: {defer_to}")
            sys.exit(1)
    elif target_env:
        if target_env in conn_ids.keys():
            connection_id = conn_ids[target_env]
        else:
            print(f"❌ Unknown target environment: {target_env}")
            sys.exit(1)
    else:
        # Default fallback
        connection_id = conn_ids.get("dev")

    if not connection_id:
        print("❌ APPLICATIVE_EXTERNAL_CONNECTION_ID is not defined.")
        sys.exit(1)

    os.environ["APPLICATIVE_EXTERNAL_CONNECTION_ID"] = connection_id
    print(
        f"✅ Set APPLICATIVE_EXTERNAL_CONNECTION_ID to {connection_id} for environment {defer_to or target_env}"
    )


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def dbt_cmd(
    ctx: typer.Context,
    command: str = typer.Argument(..., help="dbt command: run, test, etc."),
    defer_to: Optional[str] = typer.Option(
        None, help="Nom de l'environnement pour le deferral (prod/stg/dev)"
    ),
    refresh_state: bool = typer.Option(
        False, help="Forcer le rafraîchissement des artefacts"
    ),
    sync_artifacts: Optional[bool] = typer.Option(
        None, "--sync-artifacts/--no-sync-artifacts"
    ),
):
    """
    Exécute une commande dbt avec support du deferral inter-GCP project.
    """
    load_dotenv(find_dotenv())
    set_connection_id(defer_to, "local")

    # Sync artifacts si demandé
    state_path = None
    if sync_artifacts:
        for env in ["dev", "stg", "prod"]:
            typer.secho(
                f"🔄 Synchronisation des artefacts depuis {env}",
                fg=typer.colors.CYAN,
            )
            pull_artifacts(env)
        return
    if defer_to:
        state_path = f"target/defer_to/{defer_to}"
        if refresh_state or not (
            Path(f"{state_path}/manifest.json").exists()
            and Path(f"{state_path}/run_results.json").exists()
        ):
            typer.secho(
                f"🔄 Recompiler le projet en {defer_to}",
                fg=typer.colors.CYAN,
            )
            compile_project(defer_to)
            # typer.secho(
            #     f"🔄 Synchronisation des artefacts depuis {defer_to}",
            #     fg=typer.colors.CYAN,
            # )
            # sync_artifacts(defer_to)
        else:
            typer.echo(
                f"✅ Artefacts déjà présents pour {defer_to} (utiliser --refresh-state pour forcer la MAJ)"
            )

    # Construction de la commande dbt
    dbt_args = ctx.args  # args inconnus = tout ce que Typer n'a pas parsé
    full_cmd = ["dbt", command] + list(dbt_args)

    if defer_to:
        full_cmd += ["--defer", "--state", state_path, "--favor-state"]
        full_cmd += ["--vars", f"{{'ENV_SHORT_NAME': '{defer_to}'}}"]

    typer.secho(f"🚀 Exécution: {' '.join(full_cmd)}", fg=typer.colors.GREEN)
    subprocess.run(full_cmd, shell=False, stdout=None)


if __name__ == "__main__":
    app()
