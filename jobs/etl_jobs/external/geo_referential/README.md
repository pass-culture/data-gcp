# geo_referential

Imports the French geographic referential into the raw lake (`raw_<env>`) as two tables,
built from public source files pinned by vintage. dbt (`int_seed__geo_iris`) joins them with
the `region_department` and `rural_city_type_data` seeds.

Replaces the hand-uploaded seeds `geo_iris`, `epci` and `zrr`, whose mismatched vintages
produced IRIS without geometry and two different SIREN codes for the same EPCI.
(`iris_france` stays: it feeds the recommendation CloudSQL.)

## Raw tables

| raw table | grain | columns |
|---|---|---|
| `geo_iris` | one row per IRIS, plus one pseudo-IRIS per municipality (`<city_code>0000`, type `Z`) for Wallis-et-Futuna, Polynésie française and Nouvelle-Calédonie | `iris_code, iris_label, iris_type, city_code, geometry_wkt` |
| `geo_municipality` | one row per municipality and per arrondissement of Paris / Lyon / Marseille, including inhabited overseas collectivities | `city_code, municipality_code, municipality_label, department_code, region_code, territorial_authority_*, district_*, sub_district_*, epci_code, epci_label, density_level, density_label, zrr_code, zrr_label, zrr_detail, frr_code` |

Municipal attributes of an arrondissement are those of its parent municipality (`municipality_code`),
exposed as `municipality_label`. Municipalities with no EPCI get `ZZZZZZZZZ` / `Sans objet`
(INSEE convention). Both tables carry `vintage_year` (the COG year) and `imported_at`; the exact
vintages of every source are written in the BigQuery table description. Tables are replaced on
each run (`WRITE_TRUNCATE`): a referential is a snapshot, not a log.

## Sources

| source | used for | vintage option |
|---|---|---|
| IGN Contours IRIS, GeoParquet WGS84 "FRA" (métropole, DROM, 975 / 977 / 978) | IRIS geometry, codes, labels, types | `--year` |
| geo.api.gouv.fr municipality contours for 986 / 987 / 988 (no IRIS there) | pseudo-IRIS geometry | `--year` |
| INSEE Code officiel géographique (`cog_ensemble_<year>_csv.zip`: commune, commune_comer, arrondissement, canton, ctcd, mvt_commune) | municipality list and hierarchy, code changes | `--year` |
| INSEE Intercommunalité-Métropole au 01-01-`<year>` | EPCI | `--year` |
| INSEE grille communale de densité (7 niveaux) | density | `--density-year` |
| DGCL France Ruralités Revitalisation (Observatoire des territoires export) | `frr_code` | `--frr-year` |
| ANCT ZRR classification, COG 2021 (last ZRR zoning, frozen) | `zrr_*` | fixed |

INSEE moves the COG files to a new page id every year: add the new id to
`utils/config.py::COG_PAGE_IDS` when a vintage is released.

Attribute files published on an older COG than the municipality list (ZRR 2021, density grid) are
aligned on the current codes with the COG movements: a municipality with no row of its own inherits
from its predecessors — the ones it merged, or the municipality it was re-established from —,
taking the densest level for the density grid and `P` (partially classified) for the ZRR when
the predecessors disagree.

### `frr_code`

Not documented in the export; inferred from the DGCL FAQ (July 2025) and row counts:
`4` FRR socle · `5` FRR+ · `1` FRR "bénéficiaires" (ex-ZRR, transitional until 2027-12-31) ·
`3` Réunion zone spéciale d'action rurale · `2` communes nouvelles, partial · `null` not classified.

## Validation (fails the job)

- every IRIS and every overseas municipality has a geometry, `iris_code` / `city_code` unique
- every municipality of the COG (including inhabited overseas collectivities) has a geometry,
  from IGN or geo.api.gouv.fr — Paris/Lyon/Marseille through their arrondissements
- every EPCI code of the municipality composition exists in the EPCI list

## Run

```bash
make install
make test
GCP_PROJECT_ID=passculture-data-ehp make dry-run                      # download + validate only
GCP_PROJECT_ID=passculture-data-ehp make run DATASET=raw_dev          # write to BigQuery
```

Source files (~150 MB) are downloaded to a temporary directory on each run. The job needs
~1.5 GB RAM (WKT conversion of 49k IRIS polygons).

Scheduled by `import_geo_referential` **monthly**: the sources are yearly, but re-running the
whole pipeline catches a moved URL or a changed file format long before the vintage bump. Each
run rewrites the two tables with the same content (`WRITE_TRUNCATE`), so it is idempotent.

The vintages are DAG params, defaulting to the latest published ones (constants at the top of
the DAG). When a producer publishes a new vintage, bump the default there — and, for a new COG
year, add its page id to `utils/config.py::COG_PAGE_IDS`.
