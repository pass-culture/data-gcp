"""One-off conversion of the raw POI CSV into a typed Parquet file for a BigQuery load.

Usage:
    uv run python scripts/export_poi_parquet.py

Produces a Parquet file whose schema is meant to match the target table
`raw_{ENV_SHORT_NAME}.base_lieux_culturels_ouverts`, per the source's documented
column types:
- siret is forced to VARCHAR. DuckDB's default type inference makes it BIGINT,
  but it's an identifier (not a quantity) and at least one row is 11 digits
  instead of the standard 14, so an integer type risks silent misreads.
- livraison/retraits_sur_place/accueil_public_sur_rendezvous are documented as
  "booleen" but ship as "Oui"/"Non" text -- cast to real BOOLEAN.
- courriels/telephones/site_internet_et_autres_liens/accessible_au_public/
  conditions_ouverture/description are documented as "json" and every non-null
  value in the source file is valid JSON -- cast to JSON so BigQuery can load
  them as a native JSON column instead of opaque STRING.
- latitude/longitude infer as DOUBLE and date_creation/date_modification infer
  as TIMESTAMP correctly on their own. Note: the source labels longitude as
  "longitude_l93" (Lambert-93 projected meters), but the actual values range
  ~-176 to 168, i.e. WGS84 decimal degrees like latitude -- that label looks
  wrong, so no reprojection is applied here.
- every other column stays VARCHAR, matching what DuckDB already infers from
  the raw CSV.
"""

from pathlib import Path

import duckdb
import typer

# Column order matches the raw CSV header exactly.
CSV_COLUMNS = [
    "id",
    "nom_du_lieu",
    "nom_usuel_du_lieu",
    "entite_juridique_rattachement",
    "siret",
    "adresse_complete",
    "adresse_entree_public",
    "numero_et_libelle_voie",
    "complement_adresse",
    "code_postal",
    "commune",
    "autres_communes",
    "code_commune",
    "code_arrondissement",
    "departement",
    "code_departement",
    "region",
    "code_region",
    "pays",
    "latitude",
    "longitude",
    "site_internet_et_autres_liens",
    "telephones",
    "courriels",
    "domaines",
    "domaines_noms_publics",
    "sous_domaines",
    "sous_domaines_noms_publics",
    "types",
    "types_noms_publics",
    "labels",
    "labels_noms_publics",
    "annee_obtention",
    "description",
    "images",
    "mentions_legales",
    "auteur_nom_illustre",
    "identifiant_deps",
    "identifiant_origine",
    "source_origine",
    "accessible_au_public",
    "conditions_ouverture",
    "livraison",
    "retraits_sur_place",
    "accueil_public_sur_rendezvous",
    "conditions_acces",
    "date_creation",
    "date_modification",
    "coordonnees_geographiques_lieu",
    "coordonnees_geographiques_commune_lieu",
]
BOOLEAN_COLUMNS = {"livraison", "retraits_sur_place", "accueil_public_sur_rendezvous"}
JSON_COLUMNS = {
    "courriels",
    "telephones",
    "site_internet_et_autres_liens",
    "accessible_au_public",
    "conditions_ouverture",
    "description",
}


def _select_expr(column: str) -> str:
    if column in BOOLEAN_COLUMNS:
        return f"({column} = 'Oui') AS {column}"
    if column in JSON_COLUMNS:
        return f"{column}::JSON AS {column}"
    return column


def main(
    csv_path: str = typer.Option("data/base_des_lieux_culturels_ouverts.csv"),
    parquet_path: str = typer.Option("data/output/base_lieux_culturels_ouverts.parquet"),
) -> None:
    Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)
    select_columns = ", ".join(_select_expr(column) for column in CSV_COLUMNS)
    con = duckdb.connect(":memory:")
    con.execute(
        f"""
        COPY (
            SELECT {select_columns}
            FROM read_csv(
                '{csv_path}',
                delim=';',
                quote='"',
                header=true,
                all_varchar=false,
                types={{'siret': 'VARCHAR'}}
            )
        ) TO '{parquet_path}' (FORMAT PARQUET)
        """
    )
    row_count = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    typer.echo(f"Wrote {row_count} rows to {parquet_path}")


if __name__ == "__main__":
    typer.run(main)
