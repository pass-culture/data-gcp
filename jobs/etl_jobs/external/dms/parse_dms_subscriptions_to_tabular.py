import json
import logging

import gcsfs
import pandas as pd
import typer

from utils import destination_table_schema_jeunes, destination_table_schema_pro

# Constants for targets
SCHEMAS = {
    "jeunes": destination_table_schema_jeunes,
    "pro": destination_table_schema_pro,
}


def load_json_from_gcs(bucket_name, file_path, project_id):
    """Load JSON file from Google Cloud Storage."""
    fs = gcsfs.GCSFileSystem(project=project_id)
    with fs.open(f"gs://{bucket_name}/{file_path}") as json_file:
        return json.load(json_file)


def parse_api_result(updated_since, dms_target, project_id, bucket_name):
    """Parse API result based on target type."""
    logging.info(f"Start parsing API result for {dms_target} since {updated_since}")

    # Load appropriate schema
    schema = SCHEMAS.get(dms_target)
    if not schema:
        raise ValueError(f"Unsupported target: {dms_target}")

    # Initialize DataFrame
    df_applications = pd.DataFrame(columns=[col["name"] for col in schema])

    # Load JSON data
    file_path = f"dms_export/unsorted_dms_{dms_target}_{updated_since}.json"
    result = load_json_from_gcs(bucket_name, file_path, project_id)

    # Parse data
    parse_result(result, df_applications, dms_target)

    # Save results
    save_results(df_applications, dms_target, updated_since, bucket_name)


def parse_result(result, df_applications, dms_target):
    """General parser that delegates to target-specific logic."""
    logging.info(f"Parsing data for target: {dms_target}")
    for data in result["data"]:
        for node in data["demarche"]["dossiers"]["edges"]:
            dossier = node.get("node")
            if dossier:
                dossier_line = extract_common_fields(dossier)

                if dms_target == "jeunes":
                    enrich_jeunes_data(dossier, dossier_line)
                elif dms_target == "pro":
                    enrich_pro_data(dossier, dossier_line)

                df_applications.loc[len(df_applications)] = dossier_line


def extract_common_fields(dossier):
    """Extract fields common to both targets."""
    return {
        "procedure_id": dossier["demarche_id"],
        "application_id": dossier["id"],
        "application_number": dossier["number"],
        "application_archived": dossier["archived"],
        "application_status": dossier["state"],
        "last_update_at": dossier["dateDerniereModification"],
        "application_submitted_at": dossier["datePassageEnConstruction"],
        "passed_in_instruction_at": dossier["datePassageEnInstruction"],
        "processed_at": dossier["dateTraitement"],
        "instructors": "; ".join(
            [instructor["email"] for instructor in dossier.get("instructeurs", [])]
        )
        or "",
    }


def enrich_jeunes_data(dossier, dossier_line):
    """Enrich data for 'jeunes' target."""
    for champ in dossier.get("champs", []):
        if champ.get("id") == "Q2hhbXAtNTk2NDUz":
            dossier_line["applicant_department"] = champ["stringValue"]
        elif champ.get("id") == "Q2hhbXAtNTgyMjIx":
            dossier_line["applicant_postal_code"] = champ["stringValue"]


def enrich_pro_data(dossier, dossier_line):
    """Enrich data for 'pro' target."""
    demandeur = dossier.get("demandeur", {})
    if "siret" in demandeur:
        dossier_line.update(
            {
                "demandeur_siret": demandeur["siret"],
                "demandeur_naf": demandeur.get("naf"),
                "demandeur_libelleNaf": demandeur.get("libelleNaf", "").replace(
                    "\n", " "
                ),
            }
        )
        if entreprise := demandeur.get("entreprise"):
            dossier_line.update(
                {
                    "demandeur_entreprise_siren": entreprise.get("siren"),
                    "demandeur_entreprise_formeJuridique": entreprise.get(
                        "formeJuridique"
                    ),
                    "demandeur_entreprise_formeJuridiqueCode": entreprise.get(
                        "formeJuridiqueCode"
                    ),
                    "demandeur_entreprise_codeEffectifEntreprise": entreprise.get(
                        "codeEffectifEntreprise"
                    ),
                    "demandeur_entreprise_raisonSociale": entreprise.get(
                        "raisonSociale"
                    ),
                    "demandeur_entreprise_siretSiegeSocial": entreprise.get(
                        "siretSiegeSocial"
                    ),
                }
            )


def save_results(df_applications, dms_target, updated_since, bucket_name):
    """Format and save DataFrame to GCS."""
    schema = SCHEMAS[dms_target]
    for column in schema:
        col_name, col_type = column["name"], column["type"]
        if col_type == "TIMESTAMP":
            df_applications[col_name] = pd.to_datetime(
                df_applications[col_name], utc=True, errors="coerce"
            )
        elif col_type == "STRING":
            df_applications[col_name] = df_applications[col_name].astype(str)

    df_applications["update_date"] = pd.Timestamp.utcnow()
    output_path = (
        f"gs://{bucket_name}/dms_export/dms_{dms_target}_{updated_since}.parquet"
    )
    df_applications.to_parquet(output_path, engine="pyarrow", index=False)
    logging.info(f"Results saved to {output_path}")


def parse_results_to_table(
    target: str = typer.Option(None, help="pro or jeunes"),
    updated_since: str = typer.Option(None, help="updated since"),
    bucket_name: str = typer.Option(None, help="data GCS bucket name"),
    project_id: str = typer.Option(None, help="GCP project ID"),
):
    parse_api_result(updated_since, target, project_id, bucket_name)


if __name__ == "__main__":
    typer.run(parse_results_to_table)
